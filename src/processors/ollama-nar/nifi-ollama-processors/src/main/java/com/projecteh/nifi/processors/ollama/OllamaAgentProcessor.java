package com.projecteh.nifi.processors.ollama;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.*;

@Tags({"ollama", "local", "ai", "llm", "agent", "mcp"})
@CapabilityDescription("An agentic NiFi processor that interacts with Ollama and supports MCP (Model Context Protocol).")
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@WritesAttributes({
    @WritesAttribute(attribute = "ollama.agent.status", description = "Success or Failure"),
    @WritesAttribute(attribute = "ollama.usage.tokens", description = "Total tokens used"),
    @WritesAttribute(attribute = "ollama.agent.iterations", description = "Number of tool-call iterations"),
    @WritesAttribute(attribute = "ollama.reasoning_log", description = "Internal conversation history")
})
public class OllamaAgentProcessor extends AbstractProcessor {

    public static final PropertyDescriptor API_KEY = new PropertyDescriptor.Builder()
            .name("API Key")
            .description("Ollama API Key (Optional, for proxied endpoints)")
            .required(false)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor MODEL_ID = new PropertyDescriptor.Builder()
            .name("Model ID")
            .description("The Ollama model to use (e.g., llama3)")
            .required(true)
            .defaultValue("llama3")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor ENDPOINT_URL = new PropertyDescriptor.Builder()
            .name("Endpoint URL")
            .description("Ollama Chat API Endpoint URL")
            .required(true)
            .defaultValue("http://localhost:11434/api/chat")
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    public static final PropertyDescriptor SYSTEM_INSTRUCTION = new PropertyDescriptor.Builder()
            .name("System Instruction")
            .description("System instructions or persona for the model")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor CONTEXT = new PropertyDescriptor.Builder()
            .name("Context")
            .description("Additional context prepended to the user prompt")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor STATIC_SKILLS = new PropertyDescriptor.Builder()
            .name("Static Skills")
            .description("JSON definition of static tools/skills")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor MCP_SERVER_URLS = new PropertyDescriptor.Builder()
            .name("MCP Server URLs")
            .description("Comma-separated list of MCP server SSE endpoints")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor MAX_ITERATIONS = new PropertyDescriptor.Builder()
            .name("Max Iterations")
            .description("Maximum number of agentic tool-call iterations")
            .required(true)
            .defaultValue("5")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Successful agent execution")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Failed agent execution")
            .build();

    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("The original flowfile")
            .build();

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final HttpClient httpClient = HttpClient.newBuilder().build();

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(API_KEY);
        descriptors.add(MODEL_ID);
        descriptors.add(ENDPOINT_URL);
        descriptors.add(SYSTEM_INSTRUCTION);
        descriptors.add(CONTEXT);
        descriptors.add(STATIC_SKILLS);
        descriptors.add(MCP_SERVER_URLS);
        descriptors.add(MAX_ITERATIONS);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        relationships.add(REL_ORIGINAL);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final ComponentLog logger = getLogger();
        final String apiKey = context.getProperty(API_KEY).getValue();
        final String modelId = context.getProperty(MODEL_ID).getValue();
        final String endpoint = context.getProperty(ENDPOINT_URL).getValue();
        final String systemInstruction = context.getProperty(SYSTEM_INSTRUCTION).evaluateAttributeExpressions(flowFile).getValue();
        final String additionalContext = context.getProperty(CONTEXT).evaluateAttributeExpressions(flowFile).getValue();
        final String staticSkillsJson = context.getProperty(STATIC_SKILLS).evaluateAttributeExpressions(flowFile).getValue();
        final String mcpUrls = context.getProperty(MCP_SERVER_URLS).evaluateAttributeExpressions(flowFile).getValue();
        final int maxIterations = context.getProperty(MAX_ITERATIONS).asInteger();

        ArrayNode messages = null;
        try {
            messages = objectMapper.createArrayNode();

            // Read FlowFile content as user prompt
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            session.exportTo(flowFile, baos);
            String userPrompt = new String(baos.toByteArray(), StandardCharsets.UTF_8);
            if (additionalContext != null) {
                userPrompt = "Context:\n" + additionalContext + "\n\nPrompt:\n" + userPrompt;
            }

            // Prepare system instructions if present
            if (systemInstruction != null) {
                ObjectNode sysMsg = messages.addObject();
                sysMsg.put("role", "system");
                sysMsg.put("content", systemInstruction);
            }

            // Prepare user prompt
            ObjectNode userMessage = messages.addObject();
            userMessage.put("role", "user");
            userMessage.put("content", userPrompt);

            // Prepare tools
            ArrayNode ollamaTools = objectMapper.createArrayNode();

            // Add static tools
            if (staticSkillsJson != null) {
                JsonNode staticSkills = objectMapper.readTree(staticSkillsJson);
                if (staticSkills.isArray()) {
                    for (JsonNode skill : staticSkills) {
                        addOllamaTool(ollamaTools, skill);
                    }
                }
            }

            // MCP Discovery
            Map<String, String> mcpToolToServer = new HashMap<>();
            if (mcpUrls != null) {
                for (String url : mcpUrls.split(",")) {
                    try {
                        JsonNode mcpTools = fetchMcpTools(url.trim());
                        if (mcpTools != null && mcpTools.isArray()) {
                            for (JsonNode tool : mcpTools) {
                                String toolName = tool.get("name").asText();
                                addOllamaTool(ollamaTools, tool);
                                mcpToolToServer.put(toolName, url.trim());
                            }
                        }
                    } catch (Exception e) {
                        logger.warn("Failed to fetch tools from MCP server {}: {}", new Object[]{url, e.getMessage()});
                    }
                }
            }

            int iteration = 0;
            String finalResponse = null;

            while (iteration < maxIterations) {
                iteration++;

                ObjectNode requestBody = objectMapper.createObjectNode();
                requestBody.put("model", modelId);
                requestBody.put("stream", false);
                requestBody.set("messages", messages);
                if (ollamaTools.size() > 0) {
                    requestBody.set("tools", ollamaTools);
                }

                HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                        .uri(URI.create(endpoint))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(objectMapper.writeValueAsString(requestBody)));

                if (apiKey != null && !apiKey.trim().isEmpty()) {
                    requestBuilder.header("Authorization", "Bearer " + apiKey);
                }

                HttpResponse<String> response = httpClient.send(requestBuilder.build(), HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() != 200) {
                    throw new RuntimeException("Ollama API Error (" + response.statusCode() + "): " + response.body());
                }

                JsonNode root = objectMapper.readTree(response.body());
                JsonNode messageNode = root.path("message");

                // Add Ollama's response (assistant role) to messages history
                messages.add(messageNode);

                boolean hasToolCall = false;
                if (messageNode.has("tool_calls") && !messageNode.get("tool_calls").isNull()) {
                    hasToolCall = true;
                    JsonNode toolCalls = messageNode.get("tool_calls");
                    if (toolCalls.isArray()) {
                        for (JsonNode toolCall : toolCalls) {
                            String name = toolCall.path("function").path("name").asText();
                            JsonNode args = toolCall.path("function").path("arguments");
                            if (args.isTextual()) {
                                args = objectMapper.readTree(args.asText());
                            }

                            logger.info("Executing tool call: {}", new Object[]{name});

                            String result;
                            if (mcpToolToServer.containsKey(name)) {
                                result = executeMcpTool(mcpToolToServer.get(name), name, args);
                            } else {
                                result = "Error: Static tool execution not implemented in this processor. Please use MCP.";
                            }

                            ObjectNode toolResponseMsg = messages.addObject();
                            toolResponseMsg.put("role", "tool");
                            toolResponseMsg.put("name", name);
                            toolResponseMsg.put("content", result);
                        }
                    }
                }

                if (!hasToolCall) {
                    finalResponse = messageNode.path("content").asText();
                    break;
                }
            }

            if (finalResponse == null) {
                finalResponse = "Max iterations reached without final response.";
            }

            final String resultText = finalResponse;
            flowFile = session.write(flowFile, out -> out.write(resultText.getBytes(StandardCharsets.UTF_8)));
            flowFile = session.putAttribute(flowFile, "ollama.agent.status", "success");
            flowFile = session.putAttribute(flowFile, "ollama.agent.iterations", String.valueOf(iteration));
            flowFile = session.putAttribute(flowFile, "ollama.reasoning_log", objectMapper.writeValueAsString(messages));

            session.transfer(flowFile, REL_SUCCESS);
            session.transfer(session.create(flowFile), REL_ORIGINAL);

        } catch (Exception e) {
            logger.error("Ollama Agent execution failed: " + e.getMessage(), e);
            flowFile = session.putAttribute(flowFile, "ollama.agent.status", "failure");
            flowFile = session.putAttribute(flowFile, "ollama.failure", e.getMessage());

            try {
                if (messages != null) {
                    flowFile = session.putAttribute(flowFile, "ollama.failure_log", objectMapper.writeValueAsString(messages));
                }
            } catch (Exception logEx) {
                logger.warn("Could not attach failure log: " + logEx.getMessage());
            }

            session.transfer(flowFile, REL_FAILURE);
        }
    }

    private void addOllamaTool(ArrayNode ollamaTools, JsonNode toolNode) {
        ObjectNode toolObj = ollamaTools.addObject();
        toolObj.put("type", "function");
        ObjectNode funcObj = toolObj.putObject("function");
        funcObj.put("name", toolNode.path("name").asText());
        funcObj.put("description", toolNode.path("description").asText());

        JsonNode parameters = toolNode.has("parameters") ? toolNode.get("parameters") : toolNode.get("inputSchema");
        if (parameters != null && !parameters.isMissingNode()) {
            funcObj.set("parameters", parameters);
        } else {
            ObjectNode defaultSchema = objectMapper.createObjectNode();
            defaultSchema.put("type", "object");
            defaultSchema.set("properties", objectMapper.createObjectNode());
            funcObj.set("parameters", defaultSchema);
        }
    }

    private JsonNode fetchMcpTools(String serverUrl) throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(serverUrl + "/tools"))
                .GET()
                .build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() == 200) {
            return objectMapper.readTree(response.body()).get("tools");
        }
        return null;
    }

    private String executeMcpTool(String serverUrl, String name, JsonNode args) throws Exception {
        ObjectNode body = objectMapper.createObjectNode();
        body.put("name", name);
        body.set("arguments", args);

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(serverUrl + "/tools/call"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(objectMapper.writeValueAsString(body)))
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        return response.body();
    }
}
