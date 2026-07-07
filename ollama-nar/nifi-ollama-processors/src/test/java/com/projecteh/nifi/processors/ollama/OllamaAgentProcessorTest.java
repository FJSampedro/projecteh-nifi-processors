package com.projecteh.nifi.processors.ollama;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

public class OllamaAgentProcessorTest {

    @Test
    public void testOllamaAgentProperties() {
        TestRunner runner = TestRunners.newTestRunner(OllamaAgentProcessor.class);
        runner.setProperty(OllamaAgentProcessor.MODEL_ID, "llama3");
        runner.setProperty(OllamaAgentProcessor.ENDPOINT_URL, "http://localhost:11434/api/chat");
        runner.setProperty(OllamaAgentProcessor.MAX_ITERATIONS, "5");

        runner.assertValid();
    }
}
