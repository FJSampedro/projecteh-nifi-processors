package com.projecteh.nifi.processors.claude;

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

@Tags({"claude", "anthropic", "ai", "llm", "agent", "mcp"})
@CapabilityDescription("An agentic NiFi processor that interacts with Anthropic Claude and supports MCP (Model Context Protocol).")
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@WritesAttributes({
    @WritesAttribute(attribute = "claude.agent.status", description = "Success or Failure"),
    @WritesAttribute(attribute = "claude.usage.tokens", description = "Total tokens used"),
    @WritesAttribute(attribute = "claude.agent.iterations", description = "Number of tool-call iterations"),
    @WritesAttribute(attribute = "claude.reasoning_log", description = "Internal conversation history")
})
public class ClaudeAgentProcessor extends AbstractProcessor {

    public static final PropertyDescriptor API_KEY = new PropertyDescriptor.Builder()
            .name("API Key")
            .description("Anthropic Claude API Key")
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor MODEL_ID = new PropertyDescriptor.Builder()
            .name("Model ID")
            .description("The Claude model to use (e.g., claude-3-5-sonnet-20240620)")
            .required(true)
            .defaultValue("claude-3-5-sonnet-20240620")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor ENDPOINT_URL = new PropertyDescriptor.Builder()
            .name("Endpoint URL")
            .description("Anthropic Messages API Endpoint URL")
            .required(true)
            .defaultValue("https://api.anthropic.com/v1/messages")
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

            // Prepare conversation state
            ObjectNode firstUserMessage = messages.addObject();
            firstUserMessage.put("role", "user");
            firstUserMessage.put("content", userPrompt);

            // Prepare tools
            ArrayNode claudeTools = objectMapper.createArrayNode();
            
            // Add static tools
            if (staticSkillsJson != null) {
                JsonNode staticSkills = objectMapper.readTree(staticSkillsJson);
                if (staticSkills.isArray()) {
                    for (JsonNode skill : staticSkills) {
                        addClaudeTool(claudeTools, skill);
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
                                addClaudeTool(claudeTools, tool);
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
                requestBody.put("max_tokens", 2048);
                if (systemInstruction != null) {
                    requestBody.put("system", systemInstruction);
                }
                requestBody.set("messages", messages);
                if (claudeTools.size() > 0) {
                    requestBody.set("tools", claudeTools);
                }

                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(endpoint))
                        .header("Content-Type", "application/json")
                        .header("x-api-key", apiKey)
                        .header("anthropic-version", "2023-06-01")
                        .POST(HttpRequest.BodyPublishers.ofString(objectMapper.writeValueAsString(requestBody)))
                        .build();

                HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() != 200) {
                    throw new RuntimeException("Claude API Error (" + response.statusCode() + "): " + response.body());
                }

                JsonNode root = objectMapper.readTree(response.body());
                
                // Add Claude's response (assistant role) to messages history
                ObjectNode assistantMessage = messages.addObject();
                assistantMessage.put("role", "assistant");
                
                JsonNode contentArray = root.path("content");
                assistantMessage.set("content", contentArray);

                boolean hasToolCall = false;
                ArrayNode toolResultsContent = objectMapper.createArrayNode();

                for (JsonNode block : contentArray) {
                    if ("tool_use".equals(block.path("type").asText())) {
                        hasToolCall = true;
                        String callId = block.path("id").asText();
                        String name = block.path("name").asText();
                        JsonNode input = block.path("input");

                        logger.info("Executing tool call: {}", new Object[]{name});

                        String result;
                        if (mcpToolToServer.containsKey(name)) {
                            result = executeMcpTool(mcpToolToServer.get(name), name, input);
                        } else {
                            result = "Error: Static tool execution not implemented in this processor. Please use MCP.";
                        }

                        ObjectNode toolResult = toolResultsContent.addObject();
                        toolResult.put("type", "tool_result");
                        toolResult.put("tool_use_id", callId);
                        toolResult.put("content", result);
                    }
                }

                if (!hasToolCall) {
                    // Extract text content
                    for (JsonNode block : contentArray) {
                        if ("text".equals(block.path("type").asText())) {
                            finalResponse = block.path("text").asText();
                            break;
                        }
                    }
                    if (finalResponse == null) {
                        finalResponse = objectMapper.writeValueAsString(contentArray);
                    }
                    break;
                } else {
                    // Add tool result messages (user role for Claude API tool responses)
                    ObjectNode toolResponseMsg = messages.addObject();
                    toolResponseMsg.put("role", "user");
                    toolResponseMsg.set("content", toolResultsContent);
                }
            }

            if (finalResponse == null) {
                finalResponse = "Max iterations reached without final response.";
            }

            final String resultText = finalResponse;
            flowFile = session.write(flowFile, out -> out.write(resultText.getBytes(StandardCharsets.UTF_8)));
            flowFile = session.putAttribute(flowFile, "claude.agent.status", "success");
            flowFile = session.putAttribute(flowFile, "claude.agent.iterations", String.valueOf(iteration));
            flowFile = session.putAttribute(flowFile, "claude.reasoning_log", objectMapper.writeValueAsString(messages));

            session.transfer(flowFile, REL_SUCCESS);
            session.transfer(session.create(flowFile), REL_ORIGINAL);

        } catch (Exception e) {
            logger.error("Claude Agent execution failed: " + e.getMessage(), e);
            flowFile = session.putAttribute(flowFile, "claude.agent.status", "failure");
            flowFile = session.putAttribute(flowFile, "claude.failure", e.getMessage());

            try {
                if (messages != null) {
                    flowFile = session.putAttribute(flowFile, "claude.failure_log", objectMapper.writeValueAsString(messages));
                }
            } catch (Exception logEx) {
                logger.warn("Could not attach failure log: " + logEx.getMessage());
            }

            session.transfer(flowFile, REL_FAILURE);
        }
    }

    private void addClaudeTool(ArrayNode claudeTools, JsonNode toolNode) {
        ObjectNode claudeTool = claudeTools.addObject();
        claudeTool.put("name", toolNode.path("name").asText());
        claudeTool.put("description", toolNode.path("description").asText());
        
        JsonNode inputSchema = toolNode.has("inputSchema") ? toolNode.get("inputSchema") : toolNode.get("parameters");
        if (inputSchema != null && !inputSchema.isMissingNode()) {
            claudeTool.set("input_schema", inputSchema);
        } else {
            ObjectNode defaultSchema = objectMapper.createObjectNode();
            defaultSchema.put("type", "object");
            defaultSchema.set("properties", objectMapper.createObjectNode());
            claudeTool.set("input_schema", defaultSchema);
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
