package com.google.gemini.processors;

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
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

@Tags({"gemini", "google", "ai", "llm", "agent", "mcp"})
@CapabilityDescription("An agentic NiFi processor that interacts with Google Gemini and supports MCP (Model Context Protocol).")
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@WritesAttributes({
    @WritesAttribute(attribute = "gemini.agent.status", description = "Success or Failure"),
    @WritesAttribute(attribute = "gemini.usage.tokens", description = "Total tokens used"),
    @WritesAttribute(attribute = "gemini.agent.iterations", description = "Number of tool-call iterations"),
    @WritesAttribute(attribute = "gemini.reasoning_log", description = "Internal conversation history")
})
public class GeminiAgentProcessor extends AbstractProcessor {

    public static final PropertyDescriptor API_KEY = new PropertyDescriptor.Builder()
            .name("API Key")
            .description("Google Gemini API Key")
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor MODEL_ID = new PropertyDescriptor.Builder()
            .name("Model ID")
            .description("The Gemini model to use (e.g., gemini-1.5-pro)")
            .required(true)
            .defaultValue("gemini-1.5-pro")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor SYSTEM_INSTRUCTION = new PropertyDescriptor.Builder()
            .name("System Instruction")
            .description("System instructions for the model")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor CONTEXT = new PropertyDescriptor.Builder()
            .name("Context")
            .description("Additional context for the prompt")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor STATIC_SKILLS = new PropertyDescriptor.Builder()
            .name("Static Skills")
            .description("JSON definition of static tools/skills (Gemini Tools format)")
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
        String modelId = context.getProperty(MODEL_ID).getValue();
        if (modelId != null && modelId.startsWith("models/")) {
            modelId = modelId.substring("models/".length());
        }
        final String systemInstruction = context.getProperty(SYSTEM_INSTRUCTION).evaluateAttributeExpressions(flowFile).getValue();
        final String additionalContext = context.getProperty(CONTEXT).evaluateAttributeExpressions(flowFile).getValue();
        final String staticSkillsJson = context.getProperty(STATIC_SKILLS).evaluateAttributeExpressions(flowFile).getValue();
        final String mcpUrls = context.getProperty(MCP_SERVER_URLS).evaluateAttributeExpressions(flowFile).getValue();
        final int maxIterations = context.getProperty(MAX_ITERATIONS).asInteger();

        ArrayNode contents = null;
        try {
            contents = objectMapper.createArrayNode();
            // Read FlowFile content as user prompt
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            session.exportTo(flowFile, baos);
            String userPrompt = new String(baos.toByteArray(), StandardCharsets.UTF_8);
            if (additionalContext != null) {
                userPrompt = "Context: " + additionalContext + "\n\nPrompt: " + userPrompt;
            }

            // Prepare conversation state
            ObjectNode firstUserMessage = contents.addObject();
            firstUserMessage.put("role", "user");
            firstUserMessage.putObject("parts").put("text", userPrompt);

            // Prepare tools
            ArrayNode tools = objectMapper.createArrayNode();
            ArrayNode functionDeclarations = tools.addObject().putArray("function_declarations");
            
            if (staticSkillsJson != null) {
                JsonNode staticSkills = objectMapper.readTree(staticSkillsJson);
                if (staticSkills.isArray()) {
                    for (JsonNode skill : staticSkills) {
                        functionDeclarations.add(skill);
                    }
                }
            }

            // MCP Discovery (Simplified)
            Map<String, String> mcpToolToServer = new HashMap<>();
            if (mcpUrls != null) {
                for (String url : mcpUrls.split(",")) {
                    try {
                        JsonNode mcpTools = fetchMcpTools(url.trim());
                        if (mcpTools != null && mcpTools.isArray()) {
                            for (JsonNode tool : mcpTools) {
                                String toolName = tool.get("name").asText();
                                functionDeclarations.add(tool);
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
            int totalTokens = 0;

            while (iteration < maxIterations) {
                iteration++;
                
                ObjectNode requestBody = objectMapper.createObjectNode();
                requestBody.set("contents", contents);
                if (systemInstruction != null) {
                    requestBody.putObject("system_instruction").putArray("parts").addObject().put("text", systemInstruction);
                }
                if (functionDeclarations.size() > 0) {
                    requestBody.set("tools", tools);
                }

                String apiUrl = "https://generativelanguage.googleapis.com/v1beta/models/" + modelId + ":generateContent?key=" + apiKey;
                
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(apiUrl))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(objectMapper.writeValueAsString(requestBody)))
                        .build();

                HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() != 200) {
                    throw new RuntimeException("Gemini API Error (" + response.statusCode() + "): " + response.body());
                }

                JsonNode root = objectMapper.readTree(response.body());
                JsonNode candidate = root.path("candidates").get(0);
                JsonNode content = candidate.path("content");
                JsonNode parts = content.path("parts");
                
                // Add Gemini response to history
                contents.add(content);

                boolean hasToolCall = false;
                ObjectNode toolResponseContent = contents.addObject();
                toolResponseContent.put("role", "function"); // Or "model" parts with tool results in newer API versions
                // Note: Gemini 1.5 uses a specific tool response format.
                // For simplicity, we assume we need to respond to tool calls.

                ArrayNode toolResults = objectMapper.createArrayNode();

                for (JsonNode part : parts) {
                    if (part.has("function_call")) {
                        hasToolCall = true;
                        JsonNode call = part.get("function_call");
                        String name = call.get("name").asText();
                        JsonNode args = call.get("args");
                        
                        logger.info("Executing tool call: {}", new Object[]{name});
                        
                        String result;
                        if (mcpToolToServer.containsKey(name)) {
                            result = executeMcpTool(mcpToolToServer.get(name), name, args);
                        } else {
                            result = "Error: Static tool execution not implemented in this processor. Please use MCP.";
                        }

                        ObjectNode toolResult = toolResults.addObject();
                        toolResult.put("name", name);
                        toolResult.putObject("response").put("name", name).set("content", objectMapper.readTree(result));
                    }
                }

                if (!hasToolCall) {
                    finalResponse = parts.get(0).path("text").asText();
                    break;
                } else {
                    // Gemini 1.5 tool response format is slightly different
                    // We need to add a message with role 'model' (already added) and then role 'function'
                    // Actually, the format is: Model message with function_call, then User message with function_response.
                    // Let's adjust.
                    contents.remove(contents.size() - 1); // Remove the empty one we added
                    ObjectNode responseMsg = contents.addObject();
                    responseMsg.put("role", "user"); // In some versions it's 'function'
                    ArrayNode responseParts = responseMsg.putArray("parts");
                    for (JsonNode res : toolResults) {
                        responseParts.addObject().set("function_response", res);
                    }
                }
            }

            if (finalResponse == null) {
                finalResponse = "Max iterations reached without final response.";
            }

            final String resultText = finalResponse;
            flowFile = session.write(flowFile, out -> out.write(resultText.getBytes(StandardCharsets.UTF_8)));
            flowFile = session.putAttribute(flowFile, "gemini.agent.status", "success");
            flowFile = session.putAttribute(flowFile, "gemini.agent.iterations", String.valueOf(iteration));
            flowFile = session.putAttribute(flowFile, "gemini.reasoning_log", objectMapper.writeValueAsString(contents));
            
            session.transfer(flowFile, REL_SUCCESS);
            session.transfer(session.create(flowFile), REL_ORIGINAL); // Simple way to pass original

        } catch (Exception e) {
            logger.error("Agent execution failed: " + e.getMessage(), e);
            flowFile = session.putAttribute(flowFile, "gemini.agent.status", "failure");
            flowFile = session.putAttribute(flowFile, "gemini.failure", e.getMessage());
            
            // Try to capture the conversation log if available
            try {
                if (contents != null) {
                    flowFile = session.putAttribute(flowFile, "gemini.failure_log", objectMapper.writeValueAsString(contents));
                }
            } catch (Exception logEx) {
                logger.warn("Could not attach failure log: " + logEx.getMessage());
            }

            session.transfer(flowFile, REL_FAILURE);
        }
    }

    private JsonNode fetchMcpTools(String serverUrl) throws Exception {
        // MCP SSE Discovery: Usually GET /tools or similar
        // For this implementation, we assume a standard MCP-over-HTTP/SSE discovery
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
