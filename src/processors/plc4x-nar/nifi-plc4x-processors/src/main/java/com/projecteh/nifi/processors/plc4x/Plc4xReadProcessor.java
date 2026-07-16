package com.projecteh.nifi.processors.plc4x;

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
import org.apache.plc4x.java.api.messages.PlcReadRequest;
import org.apache.plc4x.java.api.messages.PlcReadResponse;
import org.apache.plc4x.java.api.types.PlcResponseCode;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;

@Tags({"plc", "plc4x", "scada", "industrial", "modbus", "s7", "ethernetip", "ads"})
@CapabilityDescription("Reads data from Industrial PLCs using Apache PLC4X. Supports S7, Modbus, EtherNet/IP, ADS, and other protocols. " +
        "Define tags to read as user-defined (dynamic) properties, where the property name is the alias and the value is the PLC address/tag.")
@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
@WritesAttributes({
    @WritesAttribute(attribute = "plc4x.read.status", description = "Success or Failure of PLC read operation"),
    @WritesAttribute(attribute = "plc4x.connection.string", description = "The connection string used to connect to the PLC")
})
@DynamicProperty(name = "Tag Alias", value = "PLC Tag Address", description = "Maps an alias to a PLC address/tag (e.g., 'temperature' -> '%DB1.DBD0:REAL')", expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
public class Plc4xReadProcessor extends AbstractProcessor {

    public static final PropertyDescriptor CONNECTION_STRING = new PropertyDescriptor.Builder()
            .name("PLC Connection String")
            .description("PLC4X Connection String (e.g. s7://192.168.1.100, modbus-tcp://192.168.1.100, ads://192.168.1.100)")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor TIMEOUT = new PropertyDescriptor.Builder()
            .name("Read Timeout")
            .description("Timeout for connecting and reading data from PLC")
            .required(true)
            .defaultValue("5000 ms")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Data successfully read from PLC")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Failed to connect or read data from PLC")
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

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null && context.hasIncomingConnection()) {
            return;
        }

        if (flowFile == null) {
            flowFile = session.create();
        }

        final ComponentLog logger = getLogger();
        final String connectionString = context.getProperty(CONNECTION_STRING).evaluateAttributeExpressions(flowFile).getValue();
        final long timeoutMs = context.getProperty(TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS);

        // Gather all dynamic properties (PLC tags)
        Map<String, String> tagsToRead = new HashMap<>();
        for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
            PropertyDescriptor descriptor = entry.getKey();
            if (descriptor.isDynamic()) {
                String alias = descriptor.getName();
                String address = context.getProperty(descriptor).evaluateAttributeExpressions(flowFile).getValue();
                tagsToRead.put(alias, address);
            }
        }

        if (tagsToRead.isEmpty()) {
            logger.warn("No PLC tags defined as dynamic properties. Routing to success with empty JSON.");
            flowFile = session.write(flowFile, out -> out.write("{}".getBytes(StandardCharsets.UTF_8)));
            flowFile = session.putAttribute(flowFile, "plc4x.read.status", "success");
            session.transfer(flowFile, REL_SUCCESS);
            return;
        }

        try {
            PlcConnection connection = getConnection(connectionString, timeoutMs);

            if (!connection.getMetadata().canRead()) {
                throw new UnsupportedOperationException("The PLC connection " + connectionString + " does not support reading.");
            }

            PlcReadRequest.Builder builder = connection.readRequestBuilder();
            for (Map.Entry<String, String> entry : tagsToRead.entrySet()) {
                builder.addTagAddress(entry.getKey(), entry.getValue());
            }

            PlcReadRequest readRequest = builder.build();
            PlcReadResponse readResponse = readRequest.execute().get(timeoutMs, TimeUnit.MILLISECONDS);

            // Construct output JSON
            ObjectNode rootNode = objectMapper.createObjectNode();
            for (String alias : readResponse.getTagNames()) {
                PlcResponseCode responseCode = readResponse.getResponseCode(alias);
                ObjectNode tagNode = rootNode.putObject(alias);
                tagNode.put("status", responseCode.name());

                if (responseCode == PlcResponseCode.OK) {
                    Object value = readResponse.getObject(alias);
                    if (value == null) {
                        tagNode.putNull("value");
                    } else if (value instanceof Boolean) {
                        tagNode.put("value", (Boolean) value);
                    } else if (value instanceof Integer) {
                        tagNode.put("value", (Integer) value);
                    } else if (value instanceof Long) {
                        tagNode.put("value", (Long) value);
                    } else if (value instanceof Float) {
                        tagNode.put("value", (Float) value);
                    } else if (value instanceof Double) {
                        tagNode.put("value", (Double) value);
                    } else if (value instanceof String) {
                        tagNode.put("value", (String) value);
                    } else {
                        tagNode.set("value", objectMapper.valueToTree(value));
                    }
                } else {
                    logger.warn("Failed to read tag '" + alias + "' with address '" + tagsToRead.get(alias) + "': " + responseCode);
                }
            }

            final String jsonResult = objectMapper.writeValueAsString(rootNode);
            flowFile = session.write(flowFile, out -> out.write(jsonResult.getBytes(StandardCharsets.UTF_8)));
            flowFile = session.putAttribute(flowFile, "plc4x.read.status", "success");
            flowFile = session.putAttribute(flowFile, "plc4x.connection.string", connectionString);

            session.transfer(flowFile, REL_SUCCESS);

        } catch (Exception e) {
            logger.error("Failed to read from PLC " + connectionString + ": " + e.getMessage(), e);
            
            closeConnection();

            flowFile = session.putAttribute(flowFile, "plc4x.read.status", "failure");
            flowFile = session.putAttribute(flowFile, "plc4x.error", e.getMessage());
            flowFile = session.putAttribute(flowFile, "plc4x.connection.string", connectionString);

            session.transfer(flowFile, REL_FAILURE);
        }
    }
}
