#!/bin/bash

echo "Waiting for NiFi to start..."
MAX_RETRIES=30
COUNT=0

until curl -k -s -f https://localhost:8443/nifi-api/access/config > /dev/null; do
    COUNT=$((COUNT + 1))
    if [ $COUNT -ge $MAX_RETRIES ]; then
        echo "NiFi failed to start in time."
        exit 1
    fi
    echo "NiFi not ready yet... ($COUNT/$MAX_RETRIES)"
    sleep 10
done

echo "NiFi is up. Checking for GeminiAgentProcessor..."

# Get access token
TOKEN=$(curl -k -s -X POST https://localhost:8443/nifi-api/access/token \
     -H "Content-Type: application/x-www-form-urlencoded" \
     -d "username=admin&password=nifipassword123")

if [ -z "$TOKEN" ]; then
    echo "Failed to get access token."
    exit 1
fi

# Search for processor type
RESULT=$(curl -k -s -X GET https://localhost:8443/nifi-api/flow/processor-types?type=GeminiAgentProcessor \
     -H "Authorization: Bearer $TOKEN")

if echo "$RESULT" | grep -q "com.google.gemini.processors.GeminiAgentProcessor"; then
    echo "SUCCESS: GeminiAgentProcessor found and loaded!"
else
    echo "FAILURE: GeminiAgentProcessor not found."
    echo "Debug: $RESULT"
    exit 1
fi
