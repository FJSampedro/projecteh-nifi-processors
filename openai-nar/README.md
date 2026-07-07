# OpenAI Apache NiFi Processor

Este procesador personalizado de Apache NiFi permite enviar prompts a la API de chat completions de **OpenAI** y recibir la respuesta directamente en el contenido del FlowFile.

## Características

- **Modular**: Se conecta directamente con la API de OpenAI.
- **Configuración de Parámetros**: Soporta especificación de modelo, temperatura, límite de tokens, system instructions y contexto.
- **Soporte de Expression Language**: Permite evaluar instrucciones de sistema y contexto usando variables del flujo de NiFi.

## Configuración del Procesador

| Propiedad | Descripción | Soporta EL? | Requerido | Valor por Defecto |
|---|---|---|---|---|
| **API Key** | Clave de API de OpenAI | No | Sí | |
| **Model ID** | Identificador del modelo (ej. `gpt-4o`, `gpt-4-turbo`) | No | Sí | `gpt-4o` |
| **Endpoint URL** | Endpoint de Chat Completions | No | Sí | `https://api.openai.com/v1/chat/completions` |
| **System Instruction** | Instrucciones de sistema o rol del modelo | Sí | No | |
| **Context** | Contexto adicional que se concatena al prompt | Sí | No | |
| **Temperature** | Parámetro de aleatoriedad (0.0 a 2.0) | No | Sí | `0.7` |
| **Max Tokens** | Cantidad máxima de tokens a generar | No | Sí | `2048` |

## Relaciones

- **success**: El FlowFile se transfiere aquí conteniendo la respuesta de OpenAI.
- **failure**: Se transfiere el FlowFile aquí si ocurre un error de conexión, autenticación o de la API de OpenAI.
