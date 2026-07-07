# Build stage
FROM maven:3.9.5-eclipse-temurin-11 AS build
WORKDIR /app

# Copy and build Gemini Processor
COPY gemini-nar/pom.xml ./gemini-nar/
COPY gemini-nar/nifi-gemini-processors ./gemini-nar/nifi-gemini-processors
COPY gemini-nar/nifi-gemini-nar ./gemini-nar/nifi-gemini-nar
RUN mvn -f gemini-nar/pom.xml clean install -DskipTests

# Copy and build PLC4X Processor
COPY plc4x-nar/pom.xml ./plc4x-nar/
COPY plc4x-nar/nifi-plc4x-processors ./plc4x-nar/nifi-plc4x-processors
COPY plc4x-nar/nifi-plc4x-nar ./plc4x-nar/nifi-plc4x-nar
RUN mvn -f plc4x-nar/pom.xml clean install -DskipTests

# Copy and build OpenAI Processor
COPY openai-nar/pom.xml ./openai-nar/
COPY openai-nar/nifi-openai-processors ./openai-nar/nifi-openai-processors
COPY openai-nar/nifi-openai-nar ./openai-nar/nifi-openai-nar
RUN mvn -f openai-nar/pom.xml clean install -DskipTests

# Copy and build Claude Processor
COPY claude-nar/pom.xml ./claude-nar/
COPY claude-nar/nifi-claude-processors ./claude-nar/nifi-claude-processors
COPY claude-nar/nifi-claude-nar ./claude-nar/nifi-claude-nar
RUN mvn -f claude-nar/pom.xml clean install -DskipTests

# Copy and build Ollama Processor
COPY ollama-nar/pom.xml ./ollama-nar/
COPY ollama-nar/nifi-ollama-processors ./ollama-nar/nifi-ollama-processors
COPY ollama-nar/nifi-ollama-nar ./ollama-nar/nifi-ollama-nar
RUN mvn -f ollama-nar/pom.xml clean install -DskipTests

# Runtime stage
FROM apache/nifi:latest
USER root

# Copy compiled NAR files to NiFi extensions directory
COPY --from=build /app/gemini-nar/nifi-gemini-nar/target/*.nar /opt/nifi/nifi-current/lib/
COPY --from=build /app/plc4x-nar/nifi-plc4x-nar/target/*.nar /opt/nifi/nifi-current/lib/
COPY --from=build /app/openai-nar/nifi-openai-nar/target/*.nar /opt/nifi/nifi-current/lib/
COPY --from=build /app/claude-nar/nifi-claude-nar/target/*.nar /opt/nifi/nifi-current/lib/
COPY --from=build /app/ollama-nar/nifi-ollama-nar/target/*.nar /opt/nifi/nifi-current/lib/

USER nifi

