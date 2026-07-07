package com.projecteh.nifi.processors.openai;

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

@Tags({"openai", "chatgpt", "llm", "ai", "text"})
@CapabilityDescription("Sends prompt from FlowFile content to OpenAI's Chat Completions API and writes the response back to the FlowFile content.")
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@WritesAttributes({
    @WritesAttribute(attribute = "openai.status", description = "Success or Failure"),
    @WritesAttribute(attribute = "openai.error", description = "Error message on failure")
})
public class OpenAiProcessor extends AbstractProcessor {

    public static final PropertyDescriptor API_KEY = new PropertyDescriptor.Builder()
            .name("API Key")
            .description("OpenAI API Key")
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor MODEL_ID = new PropertyDescriptor.Builder()
            .name("Model ID")
            .description("The OpenAI model to use (e.g., gpt-4o, gpt-4-turbo)")
            .required(true)
            .defaultValue("gpt-4o")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor ENDPOINT_URL = new PropertyDescriptor.Builder()
            .name("Endpoint URL")
            .description("OpenAI Chat Completions API Endpoint URL")
            .required(true)
            .defaultValue("https://api.openai.com/v1/chat/completions")
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

    public static final PropertyDescriptor TEMPERATURE = new PropertyDescriptor.Builder()
            .name("Temperature")
            .description("Sampling temperature (0.0 to 2.0)")
            .required(true)
            .defaultValue("0.7")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor MAX_TOKENS = new PropertyDescriptor.Builder()
            .name("Max Tokens")
            .description("The maximum number of tokens to generate in the completion")
            .required(true)
            .defaultValue("2048")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Successful call to OpenAI API")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Failed call to OpenAI API")
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
        descriptors.add(TEMPERATURE);
        descriptors.add(MAX_TOKENS);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
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
        final double temperature = Double.parseDouble(context.getProperty(TEMPERATURE).getValue());
        final int maxTokens = context.getProperty(MAX_TOKENS).asInteger();

        try {
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            session.exportTo(flowFile, baos);
            String userPrompt = new String(baos.toByteArray(), StandardCharsets.UTF_8);
            if (additionalContext != null) {
                userPrompt = "Context:\n" + additionalContext + "\n\nPrompt:\n" + userPrompt;
            }

            ObjectNode requestBody = objectMapper.createObjectNode();
            requestBody.put("model", modelId);
            requestBody.put("temperature", temperature);
            requestBody.put("max_tokens", maxTokens);

            ArrayNode messages = requestBody.putArray("messages");

            if (systemInstruction != null) {
                ObjectNode sysMsg = messages.addObject();
                sysMsg.put("role", "system");
                sysMsg.put("content", systemInstruction);
            }

            ObjectNode userMsg = messages.addObject();
            userMsg.put("role", "user");
            userMsg.put("content", userPrompt);

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(endpoint))
                    .header("Content-Type", "application/json")
                    .header("Authorization", "Bearer " + apiKey)
                    .POST(HttpRequest.BodyPublishers.ofString(objectMapper.writeValueAsString(requestBody)))
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != 200) {
                throw new RuntimeException("OpenAI API Error (" + response.statusCode() + "): " + response.body());
            }

            JsonNode root = objectMapper.readTree(response.body());
            String responseText = root.path("choices").get(0).path("message").path("content").asText();

            flowFile = session.write(flowFile, out -> out.write(responseText.getBytes(StandardCharsets.UTF_8)));
            flowFile = session.putAttribute(flowFile, "openai.status", "success");

            session.transfer(flowFile, REL_SUCCESS);

        } catch (Exception e) {
            logger.error("OpenAI request failed: " + e.getMessage(), e);
            flowFile = session.putAttribute(flowFile, "openai.status", "failure");
            flowFile = session.putAttribute(flowFile, "openai.error", e.getMessage());
            session.transfer(flowFile, REL_FAILURE);
        }
    }
}
