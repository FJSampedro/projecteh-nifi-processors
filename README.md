# Project EagleHub - Apache NiFi Custom Processors

Este repositorio contiene la colección de procesadores personalizados de **Apache NiFi** diseñados y desarrollados para el ecosistema de **Project EagleHub (Project EH)**. Este repositorio está estructurado para que cada procesador resida en su propia carpeta independiente y actúe como una fuente de compilación (*build source*) para el repositorio principal de EagleHub.

---

## 📂 Estructura del Repositorio

El repositorio se organiza de forma modular. Cada procesador personalizado se encuentra en su propia subcarpeta, la cual contiene su código fuente, configuración de compilación (Maven) y su propio archivo de documentación (`README.md`):

```text
projecteh-nifi-processors/
├── gemini-nar/                # Procesador para Gemini Agent con soporte MCP
│   ├── nifi-gemini-processors/
│   ├── nifi-gemini-nar/
│   ├── README.md
│   └── pom.xml
├── plc4x-nar/                 # Procesadores para PLCs industriales (Lectura y Escritura con PLC4X)
│   ├── nifi-plc4x-processors/
│   ├── nifi-plc4x-nar/
│   ├── README.md
│   └── pom.xml
├── openai-nar/                # Procesador para OpenAI (ChatGPT)
│   ├── nifi-openai-processors/
│   ├── nifi-openai-nar/
│   ├── README.md
│   └── pom.xml
├── claude-nar/                # Procesador para Anthropic Claude
│   ├── nifi-claude-processors/
│   ├── nifi-claude-nar/
│   ├── README.md
│   └── pom.xml
├── ollama-nar/                # Procesador para Ollama (Modelos locales)
│   ├── nifi-ollama-processors/
│   ├── nifi-ollama-nar/
│   ├── README.md
│   └── pom.xml
└── [nuevo-procesador-nar]/    # Estructura para futuros procesadores
    ├── README.md
    └── pom.xml
```

---

## 🛠️ Compilación y Empaquetado

Cada carpeta de procesador es un proyecto Maven independiente que genera un archivo **NAR (NiFi Archive)**.

### Requisitos Previos

- **Java Development Kit (JDK)**: Versión 11 o superior (según los requerimientos del procesador).
- **Apache Maven**: Versión 3.9 o superior.

### Compilación Local (Manual)

Para compilar y empaquetar un procesador específico localmente, navega a la carpeta correspondiente y ejecuta:

```bash
cd <nombre-del-procesador-nar>
mvn clean install
```

Esto generará el archivo `.nar` dentro del directorio `target/` del submódulo NAR. Por ejemplo, para el procesador Gemini:

```text
gemini-nar/nifi-gemini-nar/target/nifi-gemini-nar-1.0-SNAPSHOT.nar
```

### Compilación y Construcción mediante Docker (Recomendado)

Si no tienes Maven o Java instalados localmente, puedes compilar todos los procesadores y generar una imagen de Apache NiFi con los `.nar` preinstalados usando el Dockerfile de despliegue:

```bash
docker build -t nifi-custom-processors -f deploy/dockerfiles/Dockerfile .
```

Este comando:
1. Utiliza una imagen intermedia de Maven para compilar todos los subproyectos (`gemini-nar`, `plc4x-nar`, etc.) sin necesidad de instalar dependencias en tu máquina local.
2. Copia automáticamente los archivos `.nar` generados al directorio `/opt/nifi/nifi-current/lib/` de la imagen base de Apache NiFi.

---

## 🚀 Integración con Project EagleHub

Este repositorio funciona como la **fuente de compilación externa** para el repositorio principal de **Project EagleHub**. Los artefactos `.nar` generados aquí son integrados en el despliegue del proyecto principal a través de los siguientes métodos:

1. **Flujo de Integración Continua (CI/CD)**: El pipeline de compilación de EagleHub puede clonar este repositorio, compilar los procesadores requeridos e inyectar los archivos `.nar` resultantes en la imagen Docker o entorno de ejecución de Apache NiFi.
2. **Despliegue Manual**: Copiando directamente los archivos `.nar` compilados en el directorio `/lib` de la instancia de Apache NiFi de EagleHub y reiniciando el servicio.

---

## ➕ Cómo añadir un nuevo Procesador

Si deseas incorporar un nuevo procesador personalizado al catálogo de EagleHub, sigue estos pasos:

1. **Crear la estructura del proyecto**:
   Crea una nueva carpeta en la raíz del repositorio con el nombre del procesador (ej. `mi-procesador-nar`). Puedes utilizar los arquetipos oficiales de Apache NiFi para estructurar el proyecto Maven (`nifi-processor-bundle-archetype`).

2. **Implementar el código fuente**:
   Desarrolla la lógica en el submódulo de procesadores correspondiente (`nifi-mi-procesador-processors`).

3. **Escribir la documentación**:
   Crea un archivo `README.md` dentro de la carpeta del procesador. Es obligatorio que incluya:
   - Descripción de la funcionalidad y casos de uso.
   - Tabla de propiedades de configuración (indicando si admiten *Expression Language* y si son obligatorias).
   - Estructura de las relaciones de salida (*relationships*).
   - Requisitos y dependencias adicionales.
   - Ejemplos de uso.

4. **Registrar y probar**:
   Asegúrate de escribir pruebas unitarias robustas utilizando la clase `TestRunner` de Apache NiFi para garantizar la estabilidad del procesador antes de su integración.

---

## 📄 Licencia

Este proyecto está bajo la licencia y políticas de desarrollo internas de **Project EagleHub**.
