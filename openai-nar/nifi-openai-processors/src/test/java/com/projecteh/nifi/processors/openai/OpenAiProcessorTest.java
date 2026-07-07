package com.projecteh.nifi.processors.openai;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

public class OpenAiProcessorTest {

    @Test
    public void testOpenAiProperties() {
        TestRunner runner = TestRunners.newTestRunner(OpenAiProcessor.class);
        runner.setProperty(OpenAiProcessor.API_KEY, "test-api-key");
        runner.setProperty(OpenAiProcessor.MODEL_ID, "gpt-4o");
        runner.setProperty(OpenAiProcessor.ENDPOINT_URL, "https://api.openai.com/v1/chat/completions");
        runner.setProperty(OpenAiProcessor.TEMPERATURE, "0.5");
        runner.setProperty(OpenAiProcessor.MAX_TOKENS, "1000");

        runner.assertValid();
    }
}
