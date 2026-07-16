package com.projecteh.nifi.processors.plc4x;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class Plc4xWriteProcessorTest {

    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(Plc4xWriteProcessor.class);
    }

    @Test
    public void testProcessorProperties() {
        testRunner.setProperty(Plc4xWriteProcessor.CONNECTION_STRING, "mock:test-connection");
        testRunner.setProperty(Plc4xWriteProcessor.TIMEOUT, "5000 ms");
        testRunner.setProperty("valve_status", "%DB1.DBX0.0:BOOL");

        testRunner.assertValid();
    }

    @Test
    public void testEmptyJson() {
        testRunner.setProperty(Plc4xWriteProcessor.CONNECTION_STRING, "mock:test-connection");
        testRunner.setProperty(Plc4xWriteProcessor.TIMEOUT, "5000 ms");
        testRunner.setProperty("valve_status", "%DB1.DBX0.0:BOOL");

        testRunner.enqueue("{}");
        testRunner.run();

        testRunner.assertTransferCount(Plc4xWriteProcessor.REL_SUCCESS, 1);
        String status = testRunner.getFlowFilesForRelationship(Plc4xWriteProcessor.REL_SUCCESS).get(0).getAttribute("plc4x.write.status");
        assertTrue("skipped".equals(status));
    }
}
