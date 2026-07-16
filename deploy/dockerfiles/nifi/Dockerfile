# Build stage
FROM maven:3.9.5-eclipse-temurin-11 AS build
WORKDIR /app

# Copy and build Gemini Processor
COPY src/processors/gemini-nar/pom.xml ./src/processors/gemini-nar/
COPY src/processors/gemini-nar/nifi-gemini-processors ./src/processors/gemini-nar/nifi-gemini-processors
COPY src/processors/gemini-nar/nifi-gemini-nar ./src/processors/gemini-nar/nifi-gemini-nar
RUN mvn -f src/processors/gemini-nar/pom.xml clean install -DskipTests

# Copy and build PLC4X Processor
COPY src/processors/plc4x-nar/pom.xml ./src/processors/plc4x-nar/
COPY src/processors/plc4x-nar/nifi-plc4x-processors ./src/processors/plc4x-nar/nifi-plc4x-processors
COPY src/processors/plc4x-nar/nifi-plc4x-nar ./src/processors/plc4x-nar/nifi-plc4x-nar
RUN mvn -f src/processors/plc4x-nar/pom.xml clean install -DskipTests

# Copy and build OpenAI Processor
COPY src/processors/openai-nar/pom.xml ./src/processors/openai-nar/
COPY src/processors/openai-nar/nifi-openai-processors ./src/processors/openai-nar/nifi-openai-processors
COPY src/processors/openai-nar/nifi-openai-nar ./src/processors/openai-nar/nifi-openai-nar
RUN mvn -f src/processors/openai-nar/pom.xml clean install -DskipTests

# Copy and build Claude Processor
COPY src/processors/claude-nar/pom.xml ./src/processors/claude-nar/
COPY src/processors/claude-nar/nifi-claude-processors ./src/processors/claude-nar/nifi-claude-processors
COPY src/processors/claude-nar/nifi-claude-nar ./src/processors/claude-nar/nifi-claude-nar
RUN mvn -f src/processors/claude-nar/pom.xml clean install -DskipTests

# Copy and build Ollama Processor
COPY src/processors/ollama-nar/pom.xml ./src/processors/ollama-nar/
COPY src/processors/ollama-nar/nifi-ollama-processors ./src/processors/ollama-nar/nifi-ollama-processors
COPY src/processors/ollama-nar/nifi-ollama-nar ./src/processors/ollama-nar/nifi-ollama-nar
RUN mvn -f src/processors/ollama-nar/pom.xml clean install -DskipTests

# Runtime stage
FROM apache/nifi:latest
USER root

# Copy compiled NAR files to NiFi extensions directory
COPY --from=build /app/src/processors/gemini-nar/nifi-gemini-nar/target/*.nar /opt/nifi/nifi-current/lib/
COPY --from=build /app/src/processors/plc4x-nar/nifi-plc4x-nar/target/*.nar /opt/nifi/nifi-current/lib/
COPY --from=build /app/src/processors/openai-nar/nifi-openai-nar/target/*.nar /opt/nifi/nifi-current/lib/
COPY --from=build /app/src/processors/claude-nar/nifi-claude-nar/target/*.nar /opt/nifi/nifi-current/lib/
COPY --from=build /app/src/processors/ollama-nar/nifi-ollama-nar/target/*.nar /opt/nifi/nifi-current/lib/

USER nifi

