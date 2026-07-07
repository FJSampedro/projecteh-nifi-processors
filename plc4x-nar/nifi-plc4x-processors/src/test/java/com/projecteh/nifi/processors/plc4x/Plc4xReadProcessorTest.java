package com.projecteh.nifi.processors.plc4x;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class Plc4xReadProcessorTest {

    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(Plc4xReadProcessor.class);
    }

    @Test
    public void testProcessorProperties() {
        testRunner.setProperty(Plc4xReadProcessor.CONNECTION_STRING, "mock:test-connection");
        testRunner.setProperty(Plc4xReadProcessor.TIMEOUT, "5000 ms");
        testRunner.setProperty("temperature", "%DB1.DBD0:REAL");

        testRunner.assertValid();
    }

    @Test
    public void testEmptyTags() {
        testRunner.setProperty(Plc4xReadProcessor.CONNECTION_STRING, "mock:test-connection");
        testRunner.setProperty(Plc4xReadProcessor.TIMEOUT, "5000 ms");

        testRunner.assertValid();
        testRunner.run();

        testRunner.assertTransferCount(Plc4xReadProcessor.REL_SUCCESS, 1);
        String content = new String(testRunner.getFlowFilesForRelationship(Plc4xReadProcessor.REL_SUCCESS).get(0).toByteArray());
        assertTrue(content.contains("{}"));
    }
}
