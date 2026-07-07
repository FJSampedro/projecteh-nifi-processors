package com.projecteh.nifi.processors.claude;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

public class ClaudeProcessorTest {

    @Test
    public void testClaudeProperties() {
        TestRunner runner = TestRunners.newTestRunner(ClaudeProcessor.class);
        runner.setProperty(ClaudeProcessor.API_KEY, "test-api-key");
        runner.setProperty(ClaudeProcessor.MODEL_ID, "claude-3-5-sonnet-20240620");
        runner.setProperty(ClaudeProcessor.ENDPOINT_URL, "https://api.anthropic.com/v1/messages");
        runner.setProperty(ClaudeProcessor.TEMPERATURE, "0.5");
        runner.setProperty(ClaudeProcessor.MAX_TOKENS, "1000");

        runner.assertValid();
    }
}
