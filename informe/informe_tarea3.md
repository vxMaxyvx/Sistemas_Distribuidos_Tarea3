# Informe Tarea 3 — Procesamiento Streaming de Métricas con Apache Spark y Visualización con Elasticsearch + Kibana

**Curso:** Sistemas Distribuidos 2026-1
**Profesor:** Nicolás Hidalgo
**Integrantes:** Vicente Cataldo, Maximiliano Oliva

---

## 1. Introducción

La Tarea 1 implementó una plataforma de análisis de consultas geoespaciales sobre el dataset
*Google Open Buildings*, con una caché (Redis) para optimizar consultas repetidas. La Tarea 2
evolucionó la arquitectura incorporando **Apache Kafka** como sistema de colas de mensajes, con
mecanismos de **reintento** y **Dead Letter Queue (DLQ)** para evitar la pérdida de consultas
durante fallas temporales.

El problema que aborda esta tercera entrega es que, hasta la Tarea 2, **todas las métricas del
sistema (throughput, latencias, hit rate, reintentos) sólo existían en memoria o en logs**, lo que
imposibilita el monitoreo en tiempo real. La Tarea 3 incorpora un **pipeline de observabilidad**
desacoplado del plano de procesamiento, compuesto por:

- **Publicación de métricas** desde el Sistema de Métricas hacia un tópico Kafka dedicado
  (`metrics-topic`).
- **Apache Spark Structured Streaming**, que consume el tópico, calcula agregaciones por
  **ventanas de tiempo** y escribe los resultados en **Elasticsearch**.
- **Kibana**, que visualiza esas agregaciones en **dashboards interactivos** que se actualizan
  automáticamente.

---

## 2. Descripción de la Arquitectura

La arquitectura mantiene intacto el plano de procesamiento de la Tarea 2 y agrega, en paralelo, un
plano de observabilidad. Ambos planos se comunican únicamente a través de Kafka, lo que garantiza
el desacoplamiento.

```
            PLANO DE PROCESAMIENTO (Tarea 2)                  PLANO DE OBSERVABILIDAD (Tarea 3)
  ┌───────────────┐   queries    ┌───────────┐
  │ Gen. Tráfico  │─────────────▶│   Kafka   │
  └───────────────┘              └─────┬─────┘
                                       │ consume
                                 ┌─────▼─────┐   miss   ┌────────────────────┐
                                 │ cache_api │─────────▶│ Gen. Respuestas    │
                                 │  (Redis)  │◀─────────│ (cómputo Q1-Q5)    │
                                 └─────┬─────┘          └────────────────────┘
                                       │ HTTP /event
                                 ┌─────▼─────┐  publica   ┌──────────────┐
                                 │  Métricas │───────────▶│ metrics-topic│
                                 └───────────┘            └──────┬───────┘
                                                                 │ readStream
                                                          ┌──────▼────────────┐
                                                          │ Spark Structured   │
                                                          │ Streaming (ventanas)│
                                                          └──────┬─────────────┘
                                                                 │ foreachBatch (upsert)
                                                          ┌──────▼───────┐   ┌─────────┐
                                                          │Elasticsearch │──▶│ Kibana  │
                                                          └──────────────┘   └─────────┘
```

### 2.1 Componentes

| Componente | Tecnología | Rol |
|---|---|---|
| Generador de Tráfico | FastAPI | Genera consultas Q1-Q5 (Zipf/uniforme), las publica en `queries` |
| Sistema Caché / Consumidor | FastAPI + Redis | Consume `queries`, resuelve por caché o delega; reintentos/DLQ |
| Generador de Respuestas | FastAPI | Cómputo geoespacial; permite inyectar fallas |
| **Sistema de Métricas** | FastAPI + kafka-python | Registra cada evento **y lo publica en `metrics-topic`** |
| **Apache Spark** | Spark 3.5 Structured Streaming | Lee `metrics-topic`, agrega por ventanas, escribe en ES |
| **Elasticsearch** | ES 8.13 | Almacén indexado de las métricas agregadas |
| **Kibana** | Kibana 8.13 | Dashboards interactivos |

### 2.2 Esquema del evento publicado en `metrics-topic`

Cada evento que recibe el Sistema de Métricas se normaliza y publica con la siguiente estructura,
que contiene los campos exigidos por el enunciado (timestamp, tipo de consulta, latencia individual,
resultado de caché, reintentos y estado final):

```json
{
  "ts": 1750000000.123,
  "timestamp": "2026-06-21T18:00:00.123000+00:00",
  "event_type": "hit | miss | recovery | retry | dlq | error",
  "query_type": "Q1",
  "latency_ms": 42.7,
  "cache_hit": true,
  "was_retried": false,
  "is_retry_event": false,
  "retry_count": 0,
  "status": "success | pending | failed",
  "key": "count:Z1:conf=0.50"
}
```

### 2.3 Procesamiento en Spark Structured Streaming

El job (`spark_streaming/job.py`) realiza:

1. **Lectura** del stream desde `metrics-topic` (`format("kafka")`).
2. **Parseo** del JSON con un esquema explícito y derivación de la columna de tiempo de evento
   (`event_time = ts.cast(timestamp)`).
3. **Ventanas de tiempo deslizantes con actualización**: `window(event_time, "1 minute", "10 seconds")`
   con `withWatermark("event_time", "30 seconds")` y `outputMode("update")`. Esto produce, cada 10
   segundos, el estado de la ventana de 1 minuto, permitiendo un dashboard prácticamente en vivo.
4. **Agregaciones por ventana**:
   - `throughput_per_min` = consultas exitosas por minuto.
   - `latency_p50`, `latency_p95`, `latency_p99` mediante `percentile_approx` sobre la latencia de
     consultas exitosas.
   - `hit_rate` = `count_hit / count_success`.
   - `retry_rate` = `count_retry / (count_success + count_dlq)`.
   - `dlq_rate` = `count_dlq / (count_success + count_dlq)`.
5. **Escritura en Elasticsearch** mediante `foreachBatch`, usando el conector
   `elasticsearch-spark-30` con `es.mapping.id = window_id` y operación `upsert`. El uso de un id
   determinístico (inicio de la ventana) hace que las actualizaciones sucesivas de una misma ventana
   **sobrescriban** el documento en lugar de duplicarlo.

---

## 3. Visualización y Análisis de Métricas (Dashboards de Kibana)

> *Insertar aquí las capturas del dashboard "Tarea 3 - Monitoreo en Tiempo Real".*

El dashboard incluye cinco paneles. Para cada uno se justifica qué información entrega, qué
comportamiento permite observar y por qué es útil para monitorear una arquitectura distribuida.

### 3.1 Throughput (consultas exitosas/min)

- **Qué entrega:** el número de consultas resueltas con éxito por minuto, por ventana.
- **Qué permite observar:** la capacidad de procesamiento efectiva del sistema y su evolución
  temporal; caídas indican degradación, subidas abruptas indican picos de carga.
- **Por qué es útil:** el throughput es el indicador primario de salud de un sistema distribuido;
  permite verificar si el sistema sostiene la demanda y dimensionar la necesidad de escalamiento.

### 3.2 Latencia p50 / p95 (ms)

- **Qué entrega:** la mediana y el percentil 95 de la latencia de las consultas exitosas.
- **Qué permite observar:** la diferencia entre p50 y p95 revela la *cola* de latencia; un p95 muy
  superior al p50 indica que una fracción de las consultas sufre demoras (típicamente cache misses o
  reintentos).
- **Por qué es útil:** los percentiles, a diferencia del promedio, capturan la experiencia de los
  peores casos, que es lo que percibe el usuario en sistemas distribuidos.

### 3.3 Hit Rate

- **Qué entrega:** la proporción de consultas servidas desde la caché.
- **Qué permite observar:** el calentamiento de la caché (sube con el tiempo) y el impacto de la
  distribución del tráfico (Zipf produce mayor hit rate que uniforme); una caída brusca delata un
  *flush* de caché o un cambio de patrón de acceso.
- **Por qué es útil:** el hit rate explica directamente las mejoras de latencia y la reducción de
  carga sobre el Generador de Respuestas.

### 3.4 Retry Rate / DLQ Rate

- **Qué entrega:** la proporción de consultas que requirieron reintentos y la proporción derivada a
  la DLQ.
- **Qué permite observar:** la aparición de fallas; durante una caída del Generador de Respuestas,
  el `retry_rate` se dispara y, si la falla persiste, aparece `dlq_rate`.
- **Por qué es útil:** son indicadores tempranos de inestabilidad; permiten detectar fallas que no
  necesariamente se reflejan aún en el throughput.

### 3.5 Volumen de eventos (éxitos / errores)

- **Qué entrega:** el conteo de eventos exitosos y de error por ventana.
- **Qué permite observar:** la magnitud absoluta de la carga y de los fallos, complementando las
  tasas relativas.
- **Por qué es útil:** ayuda a distinguir, por ejemplo, un retry_rate alto con poco volumen (poco
  preocupante) de uno con volumen alto (incidente serio).

---

## 4. Análisis de Escenarios de Ejecución

> *Insertar capturas del dashboard durante cada escenario.*

### 4.1 Operación normal
Throughput estable cercano a la tasa de inyección, `hit_rate` creciente hasta estabilizarse,
`latency_p95` baja y `retry_rate`/`dlq_rate` en cero. Es la línea base contra la cual se comparan
los demás escenarios.

### 4.2 Uno vs. múltiples consumidores
Al escalar `cache_api` (p. ej. `--scale cache_api=3`), el `throughput_per_min` aumenta y la
`latency_p95` disminuye, ya que el backlog del tópico `queries` se reparte entre más consumidores.
Los dashboards permiten cuantificar la ganancia del escalamiento horizontal.

### 4.3 Falla temporal del Generador de Respuestas
**¿Es posible identificar una falla temporal sólo observando los dashboards?** Sí. Al inyectar la
falla: (1) el `throughput_per_min` cae, (2) el `retry_rate` sube de inmediato, y (3) si la caída se
prolonga, aparece `dlq_rate`. Al restaurar el servicio, el throughput se recupera y las tasas
vuelven a cero, evidenciando el mecanismo de reintentos de Kafka. La firma combinada
*throughput↓ + retry_rate↑* es inequívoca de una falla transitoria.

### 4.4 Reintentos y uso de DLQ
**¿Qué indicadores permiten detectar reintentos o degradación?** El `retry_rate` es el detector
directo. Mientras el Generador está caído, las consultas con miss se reencolan en `retry-queries`
incrementando el `retry_rate`; las que agotan `MAX_RETRIES` elevan el `dlq_rate`. La diferencia con
la arquitectura síncrona (Tarea 1) es notable: allí las consultas fallidas se perderían (errores)
sin posibilidad de recuperación.

### 4.5 Alta carga y spikes de tráfico
**¿Cómo se manifiesta un aumento repentino de carga?** Con un pico abrupto en `throughput_per_min`
acompañado de un aumento de `latency_p95` (la cola de latencia se estira por la contención). Tras el
spike, ambas métricas se normalizan a medida que se drena el backlog. Los dashboards permiten medir
el tiempo de recuperación.

---

## 5. Justificación de la Arquitectura de Monitoreo

### 5.1 ¿Por qué Apache Spark Structured Streaming?
- **Procesamiento continuo mediante ventanas temporales:** Structured Streaming ofrece de forma
  nativa ventanas (tumbling/sliding), manejo de *event-time* y *watermarking* para datos tardíos,
  lo que evita reimplementar lógica compleja de agregación temporal.
- **Escalabilidad:** el mismo job corre en `local[*]` o en un clúster sin cambios de código.
- **Tolerancia a fallos:** el *checkpointing* permite reanudar el procesamiento sin perder estado.

### 5.2 ¿Por qué Elasticsearch + Kibana?
- Elasticsearch está optimizado para **datos time-series** y consultas de agregación de baja
  latencia; Kibana se integra de forma nativa y permite construir dashboards interactivos con
  auto-refresh sin desarrollar un frontend propio.
- El conector `elasticsearch-spark` integra ambos mundos con un esfuerzo mínimo.

### 5.3 Separación entre plano de procesamiento y plano de observabilidad
El uso de un **tópico Kafka dedicado** (`metrics-topic`) desacopla por completo la observabilidad
del procesamiento: si Spark, Elasticsearch o Kibana fallan, el sistema de consultas sigue
funcionando (las métricas simplemente no se visualizan). Este desacoplamiento es una buena práctica
fundamental en arquitecturas distribuidas, pues evita que el monitoreo se convierta en un punto
único de falla del servicio principal.

### 5.4 Ventajas de un pipeline desacoplado
- Cada componente escala y evoluciona de forma independiente.
- Se pueden agregar nuevos consumidores del `metrics-topic` (p. ej. alertas) sin tocar el resto.
- La carga de cómputo de las agregaciones recae en Spark, no en el camino crítico de las consultas.

### 5.5 Desafíos de integración Kafka–Spark–Elasticsearch
- **Compatibilidad de versiones:** alinear Spark 3.5 (Scala 2.12) con los conectores
  `spark-sql-kafka-0-10` y `elasticsearch-spark-30` correctos.
- **Sink hacia Elasticsearch:** se optó por `foreachBatch` + escritura batch (más robusta que el
  sink de streaming directo) usando un *id* determinístico por ventana para evitar duplicados al
  trabajar en modo `update`.
- **Tiempos de arranque y dependencias:** Spark debe esperar a que `metrics-topic` exista y a que
  Elasticsearch esté operativo; se resolvió con `depends_on` por *healthcheck* y una espera inicial.
- **Tipos de datos en ES:** se definió un *index template* para que los campos de fecha
  (`window_end`, `window_start`, `@timestamp`) se indexen como `date` y Kibana los reconozca como
  campo temporal.

---

## 6. Discusión: Monitoreo en Tiempo Real vs. Análisis Posterior

### 6.1 Beneficios del monitoreo en línea para la detección de fallos
Frente a depender únicamente de logs o métricas locales, el monitoreo en línea permite **detectar y
caracterizar incidentes mientras ocurren**. La firma *throughput↓ + retry_rate↑* aparece en
segundos en el dashboard, mientras que con logs habría que recolectarlos y procesarlos *a
posteriori*, retrasando la respuesta.

### 6.2 Problemas identificables rápidamente mediante dashboards
- Caídas de servicio (throughput a cero, retry/DLQ al alza).
- Degradación de rendimiento (p95 creciente sin caída de throughput).
- Pérdida de eficacia de la caché (hit rate decreciente).
- Saturación por picos de carga (volumen y p95 al alza simultáneos).

### 6.3 Limitaciones y mejoras futuras
- **Latencia de visualización:** existe un retardo intrínseco igual a `watermark + slide` (decenas
  de segundos) entre que ocurre un evento y que se refleja en el dashboard; para alertas críticas
  podría reducirse la ventana a costa de mayor carga.
- **Sin alertas automáticas:** actualmente el monitoreo es visual; una mejora es añadir reglas de
  alerta (Kibana Alerting o un consumidor adicional del `metrics-topic`).
- **Retención y costo:** Elasticsearch crece con el tiempo; convendría definir políticas de
  *Index Lifecycle Management* (ILM) para datos históricos.
- **Métricas adicionales:** podría incorporarse el *backlog* del tópico `queries` como serie
  temporal para correlacionar carga encolada con latencia.

---

## 7. Conclusión

La Tarea 3 transforma un sistema que sólo registraba métricas en logs en una plataforma con
**observabilidad en tiempo real**. El pipeline `metrics-topic → Spark → Elasticsearch → Kibana`
permite no sólo visualizar el comportamiento del sistema, sino también **detectar y caracterizar**
los escenarios de la Tarea 2 (operación normal, escalamiento, fallas, reintentos/DLQ y spikes)
directamente desde los dashboards. El diseño desacoplado mediante un tópico dedicado garantiza que
la capa de monitoreo no comprometa la disponibilidad del servicio principal, una propiedad esencial
en sistemas distribuidos.
