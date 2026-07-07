package com.projecteh.nifi.processors.plc4x;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.plc4x.java.DefaultPlcDriverManager;
import org.apache.plc4x.java.api.PlcConnection;
import org.apache.plc4x.java.api.messages.PlcWriteRequest;
import org.apache.plc4x.java.api.messages.PlcWriteResponse;
import org.apache.plc4x.java.api.types.PlcResponseCode;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;

@Tags({"plc", "plc4x", "scada", "industrial", "modbus", "s7", "ethernetip", "ads", "write"})
@CapabilityDescription("Writes data to Industrial PLCs using Apache PLC4X. Supports S7, Modbus, EtherNet/IP, ADS, and other protocols. " +
        "The FlowFile content must be a JSON object containing the tag names (aliases) and their values to write. " +
        "Define tags to write as user-defined (dynamic) properties, where the property name is the alias and the value is the PLC address/tag.")
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@WritesAttributes({
    @WritesAttribute(attribute = "plc4x.write.status", description = "Success or Failure of PLC write operation"),
    @WritesAttribute(attribute = "plc4x.connection.string", description = "The connection string used to connect to the PLC")
})
@DynamicProperty(name = "Tag Alias", value = "PLC Tag Address", description = "Maps an alias in the JSON to a PLC address/tag (e.g., 'valve_setpoint' -> '%DB1.DBD8:REAL')", expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
public class Plc4xWriteProcessor extends AbstractProcessor {

    public static final PropertyDescriptor CONNECTION_STRING = new PropertyDescriptor.Builder()
            .name("PLC Connection String")
            .description("PLC4X Connection String (e.g. s7://192.168.1.100, modbus-tcp://192.168.1.100, ads://192.168.1.100)")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor TIMEOUT = new PropertyDescriptor.Builder()
            .name("Write Timeout")
            .description("Timeout for connecting and writing data to PLC")
            .required(true)
            .defaultValue("5000 ms")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Data successfully written to PLC")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Failed to connect or write data to PLC")
            .build();

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;
    private final ObjectMapper objectMapper = new ObjectMapper();

    // Cache PLC Connection
    private PlcConnection plcConnection;
    private String cachedConnectionString;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(CONNECTION_STRING);
        descriptors.add(TIMEOUT);
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
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .required(false)
                .dynamic(true)
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .build();
    }

    @OnStopped
    public synchronized void closeConnection() {
        if (plcConnection != null) {
            try {
                if (plcConnection.isConnected()) {
                    plcConnection.close();
                }
            } catch (Exception e) {
                getLogger().warn("Failed to close PLC connection: " + e.getMessage());
            } finally {
                plcConnection = null;
                cachedConnectionString = null;
            }
        }
    }

    private synchronized PlcConnection getConnection(String connectionString, long timeoutMs) throws Exception {
        if (plcConnection != null && connectionString.equals(cachedConnectionString)) {
            if (plcConnection.isConnected()) {
                return plcConnection;
            } else {
                closeConnection();
            }
        }

        getLogger().info("Establishing new PLC connection to: " + connectionString);
        plcConnection = new DefaultPlcDriverManager().getConnection(connectionString);
        cachedConnectionString = connectionString;

        if (!plcConnection.isConnected()) {
            throw new RuntimeException("Failed to connect to PLC: connection is not active");
        }

        return plcConnection;
    }

    private Object extractJavaValue(JsonNode node) {
        if (node == null || node.isNull()) {
            return null;
        }
        if (node.isBoolean()) {
            return node.asBoolean();
        }
        if (node.isIntegralNumber()) {
            if (node.canConvertToInt()) {
                return node.asInt();
            } else {
                return node.asLong();
            }
        }
        if (node.isFloatingPointNumber()) {
            return node.asDouble();
        }
        if (node.isTextual()) {
            return node.asText();
        }
        return node.toString();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final ComponentLog logger = getLogger();
        final String connectionString = context.getProperty(CONNECTION_STRING).evaluateAttributeExpressions(flowFile).getValue();
        final long timeoutMs = context.getProperty(TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS);

        // Parse FlowFile content as JSON
        final Map<String, Object> tagsToWrite = new HashMap<>();
        final Map<String, String> tagAddresses = new HashMap<>();

        // Gather all dynamic properties configured on this processor
        final Map<String, PropertyDescriptor> dynamicDescriptors = new HashMap<>();
        for (final PropertyDescriptor descriptor : context.getProperties().keySet()) {
            if (descriptor.isDynamic()) {
                dynamicDescriptors.put(descriptor.getName(), descriptor);
            }
        }

        try {
            // Read incoming JSON payload
            try (InputStream in = session.read(flowFile)) {
                JsonNode rootNode = objectMapper.readTree(in);
                if (!rootNode.isObject()) {
                    throw new IllegalArgumentException("FlowFile content must be a JSON object containing key-value pairs to write");
                }

                Iterator<Map.Entry<String, JsonNode>> fields = rootNode.fields();
                while (fields.hasNext()) {
                    Map.Entry<String, JsonNode> field = fields.next();
                    String alias = field.getKey();
                    JsonNode valueNode = field.getValue();

                    PropertyDescriptor descriptor = dynamicDescriptors.get(alias);
                    if (descriptor != null) {
                        String address = context.getProperty(descriptor).evaluateAttributeExpressions(flowFile).getValue();
                        Object val = extractJavaValue(valueNode);
                        if (val != null) {
                            tagsToWrite.put(alias, val);
                            tagAddresses.put(alias, address);
                        }
                    }
                }
            }

            if (tagsToWrite.isEmpty()) {
                logger.warn("No tags in the FlowFile JSON matched the configured dynamic properties. Transferring to success without writing.");
                flowFile = session.putAttribute(flowFile, "plc4x.write.status", "skipped");
                session.transfer(flowFile, REL_SUCCESS);
                return;
            }

            PlcConnection connection = getConnection(connectionString, timeoutMs);

            if (!connection.getMetadata().canWrite()) {
                throw new UnsupportedOperationException("The PLC connection " + connectionString + " does not support writing.");
            }

            PlcWriteRequest.Builder builder = connection.writeRequestBuilder();
            for (Map.Entry<String, Object> entry : tagsToWrite.entrySet()) {
                String alias = entry.getKey();
                String address = tagAddresses.get(alias);
                Object val = entry.getValue();
                builder.addTagAddress(alias, address, val);
            }

            PlcWriteRequest writeRequest = builder.build();
            PlcWriteResponse writeResponse = writeRequest.execute().get(timeoutMs, TimeUnit.MILLISECONDS);

            // Construct response summary JSON
            ObjectNode responseSummaryNode = objectMapper.createObjectNode();
            boolean anyFailure = false;

            for (String alias : writeResponse.getTagNames()) {
                PlcResponseCode responseCode = writeResponse.getResponseCode(alias);
                responseSummaryNode.put(alias, responseCode.name());
                if (responseCode != PlcResponseCode.OK) {
                    anyFailure = true;
                    logger.warn("Failed to write tag '" + alias + "' to address '" + tagAddresses.get(alias) + "': " + responseCode);
                }
            }

            // Write summary to FlowFile content or keep original content and add status as attribute
            // We'll keep the original content but write the status summary as an attribute
            flowFile = session.putAttribute(flowFile, "plc4x.write.status", anyFailure ? "partial_failure" : "success");
            flowFile = session.putAttribute(flowFile, "plc4x.write.results", objectMapper.writeValueAsString(responseSummaryNode));
            flowFile = session.putAttribute(flowFile, "plc4x.connection.string", connectionString);

            if (anyFailure) {
                session.transfer(flowFile, REL_FAILURE);
            } else {
                session.transfer(flowFile, REL_SUCCESS);
            }

        } catch (Exception e) {
            logger.error("Failed to write to PLC " + connectionString + ": " + e.getMessage(), e);

            closeConnection();

            flowFile = session.putAttribute(flowFile, "plc4x.write.status", "failure");
            flowFile = session.putAttribute(flowFile, "plc4x.error", e.getMessage());
            flowFile = session.putAttribute(flowFile, "plc4x.connection.string", connectionString);

            session.transfer(flowFile, REL_FAILURE);
        }
    }
}
