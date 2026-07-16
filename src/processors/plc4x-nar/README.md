# PLC4X Apache NiFi Processors

Este módulo de Apache NiFi contiene procesadores personalizados para la comunicación (lectura y escritura) con Controladores Lógicos Programables (PLCs) industriales utilizando la biblioteca **Apache PLC4X**.

Soporta protocolos industriales comunes como **Siemens S7, Modbus TCP, EtherNet/IP, Beckhoff ADS**, entre otros.

El paquete contiene dos procesadores:
1. **Plc4xReadProcessor**: Para leer datos desde la PLC.
2. **Plc4xWriteProcessor**: Para escribir datos en la PLC.

---

## Características Comunes

- **Modular y extensible**: Compatible con cualquier controlador que soporte PLC4X.
- **Configuración dinámica**: Las direcciones físicas del PLC se mapean dinámicamente utilizando propiedades personalizadas en NiFi.
- **Reutilización de conexiones**: Mantiene y reutiliza la conexión TCP del PLC entre ejecuciones del procesador, optimizando el rendimiento de la red.

---

## Instalación

### Requisitos

- Java 11 o superior
- Maven 3.9 o superior

### Compilación

Navega a esta carpeta y compila el paquete:

```bash
mvn clean install
```

Este comando generará el archivo `.nar` en:
`nifi-plc4x-nar/target/nifi-plc4x-nar-1.0-SNAPSHOT.nar`

### Despliegue

Copia el archivo `.nar` al directorio `lib/` de tu instancia de Apache NiFi y reinicia el servicio.

---

## 1. Plc4xReadProcessor (Lectura)

Lee datos del PLC y genera un JSON con el resultado.

### Propiedades Principales (Read)

| Propiedad | Descripción | Soporta EL? | Requerido | Valor por Defecto |
|---|---|---|---|---|
| **PLC Connection String** | Cadena de conexión PLC4X (ej. `s7://192.168.1.100`, `modbus-tcp://192.168.1.101`) | Sí | Sí | |
| **Read Timeout** | Límite de tiempo para la conexión y lectura de datos | No | Sí | `5000 ms` |

### Propiedades Dinámicas (Tags de Lectura)

Define propiedades dinámicas donde el **Nombre** es el alias del tag y el **Valor** es la dirección física del PLC.

#### Ejemplo de Salida (JSON)

```json
{
  "temperatura": {
    "status": "OK",
    "value": 24.57
  },
  "presion": {
    "status": "OK",
    "value": 1.23
  }
}
```

---

## 2. Plc4xWriteProcessor (Escritura)

Escribe datos en el PLC tomando los valores de un FlowFile entrante en formato JSON.

### Propiedades Principales (Write)

| Propiedad | Descripción | Soporta EL? | Requerido | Valor por Defecto |
|---|---|---|---|---|
| **PLC Connection String** | Cadena de conexión PLC4X (ej. `s7://192.168.1.100`, `modbus-tcp://192.168.1.101`) | Sí | Sí | |
| **Write Timeout** | Límite de tiempo para la conexión y escritura de datos | No | Sí | `5000 ms` |

### Propiedades Dinámicas (Tags de Escritura)

Define propiedades dinámicas donde el **Nombre** es el alias que vendrá en la clave del JSON del FlowFile entrante, y el **Valor** es la dirección física del PLC a la que se escribirá.

#### Ejemplo de Entrada (JSON en el FlowFile)

```json
{
  "consigna_temp": 26.5,
  "abrir_valvula": true
}
```

Si el procesador tiene configuradas las propiedades dinámicas:
- `consigna_temp` -> `%DB1.DBD8:REAL`
- `abrir_valvula` -> `%DB1.DBX12.0:BOOL`

Se escribirán los valores correspondientes a esas direcciones físicas en el PLC.

### Atributos de Salida (Write)

- `plc4x.write.status`: Estado general (`success`, `partial_failure`, o `failure`).
- `plc4x.write.results`: Resumen JSON detallando el código de respuesta PLC4X de cada tag escrito (ej. `{"consigna_temp":"OK","abrir_valvula":"OK"}`).

---

## Direcciones de PLC Comunes (Sintaxis)

- **Siemens S7**:
  - `%DB1.DBD0:REAL` (Bloque de datos 1, dirección 0, tipo Float de 32 bits)
  - `%DB2.DBX0.0:BOOL` (Bloque de datos 2, dirección 0, bit 0, tipo Boolean)
- **Modbus TCP**:
  - `holding-register:1:REAL` (Registro de retención 1, interpretado como Float de 32 bits)
  - `coil:5` (Coil en la posición 5)

---

## Relaciones

- **success**: Se transfieren aquí los FlowFiles que se procesaron exitosamente.
- **failure**: Se transfieren aquí los FlowFiles si la conexión falló o hubo errores de escritura/lectura en la PLC.
