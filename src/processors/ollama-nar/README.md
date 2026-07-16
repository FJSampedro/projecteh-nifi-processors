# Ollama Apache NiFi Processor

Este procesador personalizado de Apache NiFi permite enviar prompts a la API local o remota de **Ollama** (`/api/chat`) y recibir la respuesta directamente en el contenido del FlowFile.

## Características

- **Modular**: Se conecta directamente con la API local o remota de Ollama sin requerir claves de API.
- **Configuración de Parámetros**: Soporta especificación de modelo, temperatura, límite de tokens (`num_predict`), system instructions y contexto.
- **Soporte de Expression Language**: Permite evaluar instrucciones de sistema y contexto usando variables del flujo de NiFi.

## Configuración del Procesador

| Propiedad | Descripción | Soporta EL? | Requerido | Valor por Defecto |
|---|---|---|---|---|
| **Model ID** | Nombre del modelo local descargado en Ollama (ej. `llama3`, `mistral`) | No | Sí | `llama3` |
| **Endpoint URL** | Endpoint del servicio local de chat de Ollama | No | Sí | `http://localhost:11434/api/chat` |
| **System Instruction** | Instrucciones de sistema o rol del modelo | Sí | No | |
| **Context** | Contexto adicional que se concatena al prompt | Sí | No | |
| **Temperature** | Parámetro de aleatoriedad (0.0 a 1.0) | No | Sí | `0.7` |
| **Max Tokens** | Cantidad máxima de tokens a generar (num_predict) | No | Sí | `2048` |

## Relaciones

- **success**: El FlowFile se transfiere aquí conteniendo la respuesta de Ollama.
- **failure**: Se transfiere el FlowFile aquí si ocurre un error de conexión o de la API de Ollama.
