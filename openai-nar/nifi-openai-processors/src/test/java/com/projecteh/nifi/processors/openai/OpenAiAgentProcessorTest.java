package com.projecteh.nifi.processors.openai;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

public class OpenAiAgentProcessorTest {

    @Test
    public void testOpenAiAgentProperties() {
        TestRunner runner = TestRunners.newTestRunner(OpenAiAgentProcessor.class);
        runner.setProperty(OpenAiAgentProcessor.API_KEY, "test-api-key");
        runner.setProperty(OpenAiAgentProcessor.MODEL_ID, "gpt-4o");
        runner.setProperty(OpenAiAgentProcessor.ENDPOINT_URL, "https://api.openai.com/v1/chat/completions");
        runner.setProperty(OpenAiAgentProcessor.MAX_ITERATIONS, "5");

        runner.assertValid();
    }
}
