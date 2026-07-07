# Gemini Agent NiFi Processor

This custom Apache NiFi processor enables agentic workflows using Google Gemini, with native support for the Model Context Protocol (MCP).

## Features

- **Agentic Loop**: Automatically handles multiple tool-call iterations until a final response is generated.
- **MCP Integration**: Connects to MCP servers (via SSE) to dynamically discover and execute tools.
- **Static Skills**: Allows defining local tools using Gemini's function declaration format in JSON.
- **NiFi Native**: Full support for Expression Language and Parameters.

## Installation

### Prerequisites

- Java 11 or higher
- Maven 3.9+
- Docker (optional, for containerized deployment)

### Build from source

```bash
mvn clean install
```

This will generate a `.nar` file in `nifi-gemini-nar/target/nifi-gemini-nar-1.0-SNAPSHOT.nar`.

### Manual Deployment

Copy the `.nar` file to your NiFi `lib/` directory and restart NiFi.

### Docker Deployment

Use the provided `Dockerfile` to build an image with the processor pre-installed:

```bash
docker build -t nifi-gemini .
docker-compose up -d
```

## Configuration

| Property | Description | EL? |
|----------|-------------|-----|
| **API Key** | Your Google Gemini API Key | No |
| **Model ID** | Gemini model (e.g., `gemini-1.5-pro`) | No |
| **System Instruction** | Base instructions for the agent | Yes |
| **Context** | Additional context for the prompt | Yes |
| **Static Skills** | JSON array of Gemini function definitions | Yes |
| **MCP Server URLs** | Comma-separated SSE endpoints | Yes |
| **Max Iterations** | Max steps for the agent loop | No |

### MCP Example

Configure `MCP Server URLs` with `http://mcp-server:8080`. The processor will:
1. Fetch tools from `http://mcp-server:8080/tools`.
2. Execute tool calls via `http://mcp-server:8080/tools/call`.

## Verification

Run the verification script to ensure the processor is loaded correctly:

```bash
./verify_processor.sh
```
