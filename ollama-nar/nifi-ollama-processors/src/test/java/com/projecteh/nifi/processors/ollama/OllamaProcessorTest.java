package com.projecteh.nifi.processors.ollama;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

public class OllamaProcessorTest {

    @Test
    public void testOllamaProperties() {
        TestRunner runner = TestRunners.newTestRunner(OllamaProcessor.class);
        runner.setProperty(OllamaProcessor.MODEL_ID, "llama3");
        runner.setProperty(OllamaProcessor.ENDPOINT_URL, "http://localhost:11434/api/chat");
        runner.setProperty(OllamaProcessor.TEMPERATURE, "0.5");
        runner.setProperty(OllamaProcessor.MAX_TOKENS, "1000");

        runner.assertValid();
    }
}
