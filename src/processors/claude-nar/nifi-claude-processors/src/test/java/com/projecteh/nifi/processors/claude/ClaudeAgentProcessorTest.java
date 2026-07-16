package com.projecteh.nifi.processors.claude;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

public class ClaudeAgentProcessorTest {

    @Test
    public void testClaudeAgentProperties() {
        TestRunner runner = TestRunners.newTestRunner(ClaudeAgentProcessor.class);
        runner.setProperty(ClaudeAgentProcessor.API_KEY, "test-api-key");
        runner.setProperty(ClaudeAgentProcessor.MODEL_ID, "claude-3-5-sonnet-20240620");
        runner.setProperty(ClaudeAgentProcessor.ENDPOINT_URL, "https://api.anthropic.com/v1/messages");
        runner.setProperty(ClaudeAgentProcessor.MAX_ITERATIONS, "5");

        runner.assertValid();
    }
}
