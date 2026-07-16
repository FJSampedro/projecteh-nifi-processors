# Anthropic Claude Apache NiFi Processor

Este procesador personalizado de Apache NiFi permite enviar prompts a la API de mensajes de **Anthropic Claude** y recibir la respuesta directamente en el contenido del FlowFile.

## Características

- **Modular**: Se conecta directamente con la API de Anthropic Claude.
- **Configuración de Parámetros**: Soporta especificación de modelo, temperatura, límite de tokens, system instructions y contexto.
- **Soporte de Expression Language**: Permite evaluar instrucciones de sistema y contexto usando variables del flujo de NiFi.

## Configuración del Procesador

| Propiedad | Descripción | Soporta EL? | Requerido | Valor por Defecto |
|---|---|---|---|---|
| **API Key** | Clave de API de Anthropic Claude | No | Sí | |
| **Model ID** | Identificador del modelo (ej. `claude-3-5-sonnet-20240620`, `claude-3-opus-20240229`) | No | Sí | `claude-3-5-sonnet-20240620` |
| **Endpoint URL** | Endpoint de la API de mensajes | No | Sí | `https://api.anthropic.com/v1/messages` |
| **System Instruction** | Instrucciones de sistema o rol del modelo | Sí | No | |
| **Context** | Contexto adicional que se concatena al prompt | Sí | No | |
| **Temperature** | Parámetro de aleatoriedad (0.0 a 1.0) | No | Sí | `0.7` |
| **Max Tokens** | Cantidad máxima de tokens a generar | No | Sí | `2048` |

## Relaciones

- **success**: El FlowFile se transfiere aquí conteniendo la respuesta de Claude.
- **failure**: Se transfiere el FlowFile aquí si ocurre un error de conexión, autenticación o de la API de Claude.
