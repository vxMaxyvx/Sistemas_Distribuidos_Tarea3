# Tarea 2 — Sistemas Distribuidos 2026-1
### Análisis de consultas geoespaciales asíncrono con Apache Kafka y Caché

**Integrantes:** Vicente Cataldo, Maximiliano Oliva  
**Stack:** Python 3.12, FastAPI, Apache Kafka (KRaft), Redis 7.4, Docker Compose v2

Esta tarea extiende la arquitectura síncrona de la Tarea 1 incorporando **Apache Kafka** como capa de mensajería asíncrona. Las consultas geoespaciales (Q1-Q5) se desacoplan completamente del procesamiento usando tópicos Kafka, con reintentos automáticos y Dead Letter Queue (DLQ) para el manejo de fallos.

---

## Lo que necesitás para correr esto

- **Docker Engine v24+** y **Docker Compose v2** (el comando es `docker compose`, no `docker-compose`)
- **Python 3.10+** con `pip` — solo para los scripts de experimentos y gráficos, los servicios corren en Docker
- El dataset `data/buildings_rm.csv` (ver sección de preparación más abajo)

Verificá que todo esté instalado antes de empezar:

```bash
docker --version        # v24.x o superior
docker compose version  # v2.x
python3 --version       # 3.10 o superior
```

Los scripts de experimentos y gráficos necesitan dos paquetes Python:

```bash
pip install numpy matplotlib
```

---

## Arquitectura

El sistema tiene 4 servicios propios más Redis y Kafka como infraestructura:

| Servicio | Puerto (host) | Rol |
|---|---|---|
| `generador_trafico` | 5003 | Productor Kafka o cliente HTTP según `USE_KAFKA` |
| `cache_api` | — (interno) | Consumidor Kafka, caché con Redis, escala horizontal |
| `generador_respuestas` | 5001 | Resuelve consultas Q1-Q5, expone `/toggle_failure` |
| `metricas` | 5002 | Registra eventos, calcula percentiles, obtiene lag Kafka |
| `redis` | 6380 | Caché en memoria con políticas LRU/LFU/FIFO |
| `kafka` | 9092 | Broker con tópicos `queries`, `retry-queries`, `dlq-queries` |

El flujo en modo Kafka: `generador_trafico` publica en `queries` → `cache_api` consume, verifica Redis, si hay miss llama a `generador_respuestas` → si falla, reintenta vía `retry-queries` hasta `MAX_RETRIES`, luego envía a `dlq-queries`.

---

## Estructura del Repositorio

```
.
├── cache_api/                       # Servicio 2 — Caché & Consumidor Kafka
│   ├── app/
│   │   ├── main.py                  # Consumer loop, /stats, /flush, /health
│   │   └── cache.py                 # Cliente de caché LRU/LFU/FIFO
│   └── Dockerfile
├── generador_respuestas/            # Servicio 3 — Cómputo Geoespacial
│   ├── app/
│   │   ├── main.py                  # /query, /toggle_failure, /health
│   │   ├── data_loader.py           # Carga en memoria del dataset
│   │   └── queries.py               # Algoritmos Q1-Q5
│   └── Dockerfile
├── generador_trafico/               # Servicio 1 — Productor de Tráfico
│   ├── app/
│   │   ├── main.py                  # /run, /stop, /status, /health
│   │   └── distributions.py         # Zipf, Uniforme, Poisson
│   └── Dockerfile
├── metricas/                        # Servicio 4 — Métricas
│   ├── app/
│   │   └── main.py                  # /event, /summary, /snapshot, /reset
│   └── Dockerfile
├── experiments/
│   ├── run_kafka_experiments.py     # Corre los 8 escenarios en secuencia automática
│   └── build_kafka_figures.py       # Genera los 8 gráficos del informe
├── scripts/
│   └── download_data.py             # Genera el dataset sintético de edificaciones
├── data/
│   └── buildings_rm.csv             # Dataset de la Región Metropolitana
├── docker-compose.yml
├── .env                             # Variables de configuración
└── README.md
```

---

## Preparación inicial

### Dataset

El generador de respuestas necesita el archivo `data/buildings_rm.csv`. La forma más fácil de generarlo es con el script incluido:

```bash
pip install numpy
python3 scripts/download_data.py
```

Esto crea ~43.000 edificaciones sintéticas basadas en las distribuciones del dataset real de Google Open Buildings para Santiago.

Si tienes el archivo original comprimido `967_buildings.csv.gz`, puedes usarlo en su lugar:

```bash
mkdir -p data
cp /ruta/al/archivo/967_buildings.csv.gz data/
pip install pandas
python3 filtrar_real.py
```

### Variables de entorno

El archivo `.env` controla el comportamiento del sistema. Las dos variables más importantes para los experimentos son:

```ini
USE_KAFKA=true   # true = modo asíncrono Kafka | false = modo síncrono HTTP
MAX_RETRIES=3    # intentos antes de enviar una consulta a la DLQ
```

---

## Levantar y bajar el entorno

```bash
# Levantar todos los servicios (modo Kafka activo por defecto)
docker compose up -d --build

# Ver logs en tiempo real
docker compose logs -f

# Bajar todo
docker compose down
```

La primera vez que levantás puede tardar 2-3 minutos mientras Kafka arranca y pasa los healthchecks. Los demás servicios esperan automáticamente a que Kafka esté listo antes de conectarse.

---

## Ejecutar los experimentos

Hay dos formas: correr todo el pipeline automáticamente o correr cada escenario a mano.

### Automático (todos los 8 escenarios en secuencia)

```bash
python experiments/run_kafka_experiments.py
```

El script reinicia los contenedores, configura `USE_KAFKA` y el número de consumidores según cada escenario, inyecta fallas cuando corresponde y guarda los snapshots en `results/`. Tarda aproximadamente **25-35 minutos** en completar los 8 escenarios.

---

### Manual (un escenario a la vez)

Cada escenario sigue el mismo patrón:
1. Levantar Docker con la configuración correcta
2. Resetear métricas y vaciar la caché
3. Lanzar el tráfico via `curl`
4. (Para escenarios de falla) Inyectar y restaurar el fallo en los tiempos indicados
5. Guardar el snapshot de resultados

---

#### Escenario 1 — Sistema Base Síncrono

Sin Kafka. Sirve de línea base para comparar contra la arquitectura asíncrona.

```bash
docker compose down
export USE_KAFKA=false
docker compose up -d --build

# Resetear estado
curl -s -X POST http://localhost:5002/reset
docker compose exec redis redis-cli FLUSHDB

# Lanzar tráfico: 100 QPS, 120 segundos, distribución Zipf
curl -s -X POST http://localhost:5003/run \
  -H "Content-Type: application/json" \
  -d '{"distribution":"zipf","rate_qps":100,"duration_sec":120,"zipf_s":1.2,"concurrency":33,"seed":42,"label":"1_sync_base"}'

# Verificar estado del tráfico
curl -s http://localhost:5003/status

# Cuando termine, guardar snapshot
curl -s -X POST http://localhost:5002/snapshot \
  -H "Content-Type: application/json" \
  -d '{"label":"1_sync_base","extra":{"use_kafka":false,"scale":1}}'
```

> **Importante:** el `POST /run` arranca el tráfico en background. Esperá a que termine (revisá `curl -s http://localhost:5003/status` hasta que diga `"running": false`) antes de ejecutar el `snapshot`. Si no, el snapshot va a salir vacío.

---

#### Escenario 2 — Kafka con 1 consumidor

Procesamiento asíncrono básico. Un solo consumer leyendo del tópico `queries`.

```bash
docker compose down
export USE_KAFKA=true
docker compose up -d --build

# Esperar ~15s para que Kafka asigne particiones al consumer
sleep 15

curl -s -X POST http://localhost:5002/reset
docker compose exec redis redis-cli FLUSHDB

curl -s -X POST http://localhost:5003/run \
  -H "Content-Type: application/json" \
  -d '{"distribution":"zipf","rate_qps":100,"duration_sec":120,"zipf_s":1.2,"concurrency":33,"seed":42,"label":"2_kafka_1_consumer"}'

# Esperar que el tráfico termine Y que el backlog de Kafka llegue a 0
# Revisarlo con: curl -s http://localhost:5002/summary

curl -s -X POST http://localhost:5002/snapshot \
  -H "Content-Type: application/json" \
  -d '{"label":"2_kafka_1_consumer","extra":{"use_kafka":true,"scale":1}}'
```

> En modo Kafka, además de esperar que el tráfico termine (`status` con `"running": false`), hay que esperar a que el backlog de Kafka llegue a 0: `curl -s http://localhost:5002/summary | grep backlog_size`. Si se toma el snapshot antes, queda vacío.

---

#### Escenario 3a — Kafka con 3 consumidores

```bash
docker compose down
export USE_KAFKA=true
docker compose up -d --build --scale cache_api=3

sleep 15

curl -s -X POST http://localhost:5002/reset
docker compose exec redis redis-cli FLUSHDB

curl -s -X POST http://localhost:5003/run \
  -H "Content-Type: application/json" \
  -d '{"distribution":"zipf","rate_qps":100,"duration_sec":120,"zipf_s":1.2,"concurrency":33,"seed":42,"label":"3a_kafka_3_consumers"}'

curl -s -X POST http://localhost:5002/snapshot \
  -H "Content-Type: application/json" \
  -d '{"label":"3a_kafka_3_consumers","extra":{"use_kafka":true,"scale":3}}'
```

> Esperar a que el tráfico termine y el `backlog_size` sea 0 antes del snapshot.

---

#### Escenario 3b — Kafka con 5 consumidores

```bash
docker compose down
export USE_KAFKA=true
docker compose up -d --build --scale cache_api=5

sleep 15

curl -s -X POST http://localhost:5002/reset
docker compose exec redis redis-cli FLUSHDB

curl -s -X POST http://localhost:5003/run \
  -H "Content-Type: application/json" \
  -d '{"distribution":"zipf","rate_qps":100,"duration_sec":120,"zipf_s":1.2,"concurrency":33,"seed":42,"label":"3b_kafka_5_consumers"}'

curl -s -X POST http://localhost:5002/snapshot \
  -H "Content-Type: application/json" \
  -d '{"label":"3b_kafka_5_consumers","extra":{"use_kafka":true,"scale":5}}'
```

> Esperar a que el tráfico termine y el `backlog_size` sea 0 antes del snapshot.

---

#### Escenario 4 — Falla temporal con Kafka (1 consumidor)

Simula una caída del Generador de Respuestas de 30 segundos. Hay que inyectar la falla a los 30s del inicio del tráfico y restaurarla a los 60s. Se recomienda tener dos terminales abiertas.

```bash
docker compose down
export USE_KAFKA=true
docker compose up -d --build

sleep 15

curl -s -X POST http://localhost:5002/reset
docker compose exec redis redis-cli FLUSHDB

# Terminal 1: lanzar tráfico
curl -s -X POST http://localhost:5003/run \
  -H "Content-Type: application/json" \
  -d '{"distribution":"zipf","rate_qps":100,"duration_sec":120,"zipf_s":1.2,"concurrency":33,"seed":42,"label":"4_kafka_transient_failure"}'

# Terminal 2: a los 30s, activar falla + vaciar caché para forzar misses
sleep 30
curl -s -X POST http://localhost:5001/toggle_failure \
  -H "Content-Type: application/json" -d '{"enabled": true}'
docker compose exec redis redis-cli FLUSHDB

# Terminal 2: a los 60s (30s más), restaurar el servicio
sleep 30
curl -s -X POST http://localhost:5001/toggle_failure \
  -H "Content-Type: application/json" -d '{"enabled": false}'

# Esperar que el tráfico termine y el backlog baje a 0, luego snapshot
curl -s -X POST http://localhost:5002/snapshot \
  -H "Content-Type: application/json" \
  -d '{"label":"4_kafka_transient_failure","extra":{"use_kafka":true,"scale":1,"simulated_failure":true}}'
```

> **Nota:** El `POST /run` es asíncrono. Esperar a que `status` diga `"running": false` y el `backlog_size` sea 0 antes del snapshot. El `backlog_history` que usa `fig4` lo genera el script automático mientras monitorea el experimento en tiempo real. Al correr este escenario manualmente, el snapshot no incluye ese historial, por lo que `fig4` no se va a poder graficar. Para generarla correctamente hay que usar el script automático.

---

#### Escenario 5 — Falla temporal síncrona (sin Kafka)

Mismo timing que el Escenario 4, pero sin Kafka. Las consultas que caen durante la falla se pierden directamente (no hay colas de reintento).

```bash
docker compose down
export USE_KAFKA=false
docker compose up -d --build

curl -s -X POST http://localhost:5002/reset
docker compose exec redis redis-cli FLUSHDB

# Terminal 1: tráfico
curl -s -X POST http://localhost:5003/run \
  -H "Content-Type: application/json" \
  -d '{"distribution":"zipf","rate_qps":100,"duration_sec":120,"zipf_s":1.2,"concurrency":33,"seed":42,"label":"5_sync_transient_failure"}'

# Terminal 2: falla a los 30s
sleep 30
curl -s -X POST http://localhost:5001/toggle_failure \
  -H "Content-Type: application/json" -d '{"enabled": true}'
docker compose exec redis redis-cli FLUSHDB

# Restaurar a los 60s
sleep 30
curl -s -X POST http://localhost:5001/toggle_failure \
  -H "Content-Type: application/json" -d '{"enabled": false}'

curl -s -X POST http://localhost:5002/snapshot \
  -H "Content-Type: application/json" \
  -d '{"label":"5_sync_transient_failure","extra":{"use_kafka":false,"scale":1,"simulated_failure":true}}'
```

> Esperar a que el tráfico termine (`"running": false`) antes del snapshot.

---

#### Escenario 6 — Spike de tráfico (3 fases)

Tres fases encadenadas: tráfico normal → spike × 4 → drenado del backlog. Hay que esperar que cada fase termine antes de lanzar la siguiente.

```bash
docker compose down
export USE_KAFKA=true
docker compose up -d --build

sleep 15

curl -s -X POST http://localhost:5002/reset
docker compose exec redis redis-cli FLUSHDB

# Fase 1: tráfico normal (80 QPS, 35 segundos)
curl -s -X POST http://localhost:5003/run \
  -H "Content-Type: application/json" \
  -d '{"distribution":"zipf","rate_qps":80,"duration_sec":35,"zipf_s":1.2,"concurrency":26,"seed":42,"label":"6_kafka_traffic_spike"}'

sleep 38

# Fase 2: spike (320 QPS, 15 segundos)
curl -s -X POST http://localhost:5003/run \
  -H "Content-Type: application/json" \
  -d '{"distribution":"zipf","rate_qps":320,"duration_sec":15,"zipf_s":1.2,"concurrency":106,"seed":42,"label":"6_kafka_traffic_spike"}'

sleep 18

# Fase 3: drenado (80 QPS, 60 segundos)
curl -s -X POST http://localhost:5003/run \
  -H "Content-Type: application/json" \
  -d '{"distribution":"zipf","rate_qps":80,"duration_sec":60,"zipf_s":1.2,"concurrency":26,"seed":42,"label":"6_kafka_traffic_spike"}'

# Esperar que termine y el backlog llegue a 0
sleep 65

curl -s -X POST http://localhost:5002/snapshot \
  -H "Content-Type: application/json" \
  -d '{"label":"6_kafka_traffic_spike","extra":{"use_kafka":true,"scale":1,"simulated_failure":false}}'
```

> En este escenario el tráfico ya terminó al momento del snapshot por los `sleep`, pero siempre conviene verificar que `backlog_size` sea 0.

---

#### Escenario 7 — Recuperación con 3 consumidores

Igual al Escenario 4 pero con 3 consumers. Mide cuánto más rápido se vacía el backlog post-falla cuando hay más workers procesando.

```bash
docker compose down
export USE_KAFKA=true
docker compose up -d --build --scale cache_api=3

sleep 15

curl -s -X POST http://localhost:5002/reset
docker compose exec redis redis-cli FLUSHDB

# Terminal 1: tráfico
curl -s -X POST http://localhost:5003/run \
  -H "Content-Type: application/json" \
  -d '{"distribution":"zipf","rate_qps":100,"duration_sec":120,"zipf_s":1.2,"concurrency":33,"seed":42,"label":"7_kafka_recovery_scaled"}'

# Terminal 2: falla a los 30s
sleep 30
curl -s -X POST http://localhost:5001/toggle_failure \
  -H "Content-Type: application/json" -d '{"enabled": true}'
docker compose exec redis redis-cli FLUSHDB

sleep 30
curl -s -X POST http://localhost:5001/toggle_failure \
  -H "Content-Type: application/json" -d '{"enabled": false}'

curl -s -X POST http://localhost:5002/snapshot \
  -H "Content-Type: application/json" \
  -d '{"label":"7_kafka_recovery_scaled","extra":{"use_kafka":true,"scale":3,"simulated_failure":true}}'
```

> Esperar a que termine el tráfico y el `backlog_size` sea 0 antes del snapshot.

---

#### Escenario 8 — Distribución Uniforme con Kafka

Mismo setup que el Escenario 2 pero con distribución uniforme. Compara hit rate y latencia cuando el tráfico no tiene sesgo (vs Zipf que favorece las consultas más populares).

```bash
docker compose down
export USE_KAFKA=true
docker compose up -d --build

sleep 15

curl -s -X POST http://localhost:5002/reset
docker compose exec redis redis-cli FLUSHDB

curl -s -X POST http://localhost:5003/run \
  -H "Content-Type: application/json" \
  -d '{"distribution":"uniform","rate_qps":100,"duration_sec":120,"zipf_s":1.2,"concurrency":33,"seed":42,"label":"8_kafka_uniform"}'

curl -s -X POST http://localhost:5002/snapshot \
  -H "Content-Type: application/json" \
  -d '{"label":"8_kafka_uniform","extra":{"use_kafka":true,"scale":1}}'
```

> Esperar a que termine el tráfico y el `backlog_size` sea 0 antes del snapshot.

---

## Generar los 8 gráficos

Una vez que todos los experimentos terminaron y los snapshots están en `results/`, correr:

```bash
python experiments/build_kafka_figures.py
```

Los archivos se generan en `informe/figs/` en formato PDF y PNG:

| Archivo | Qué muestra | Snapshots que necesita |
|---|---|---|
| `fig1_throughput_comparison` | Throughput: síncrono vs Kafka vs Kafka escalado | `1_sync_base`, `2_kafka_1_consumer`, `3a_kafka_3_consumers` |
| `fig2_latency_comparison` | Percentiles p50/p95 en escala logarítmica | `1_sync_base`, `2_kafka_1_consumer`, `3a_kafka_3_consumers` |
| `fig3_reliability_comparison` | Consultas completadas vs perdidas ante caída de 30s | `5_sync_transient_failure`, `4_kafka_transient_failure` |
| `fig4_backlog_evolution` | Evolución del backlog durante la falla y recuperación | `4_kafka_transient_failure` (con historial de backlog) |
| `fig5_retry_dlq_rates` | Tasa de reintentos y DLQ: 1 vs 3 consumidores | `4_kafka_transient_failure`, `7_kafka_recovery_scaled` |
| `fig6_spike_backlog` | Acumulación de backlog en el spike de tráfico | `6_kafka_traffic_spike` (con historial de backlog) |
| `fig7_scaling_consumers` | Throughput y latencia según número de consumidores | `2_kafka_1_consumer`, `3a_kafka_3_consumers`, `3b_kafka_5_consumers` |
| `fig8_distribution_comparison` | Hit rate y latencia mediana: Zipf vs Uniforme | `2_kafka_1_consumer`, `8_kafka_uniform` |

> `fig4` y `fig6` requieren que los escenarios 4 y 6 hayan sido corridos con el **script automático**, ya que el historial de backlog se construye mientras el script monitorea la ejecución en tiempo real. Al correr manualmente esos snapshots no incluyen ese historial.

---

## Escalamiento en caliente

Si el entorno ya está corriendo y querés agregar más consumidores sin reiniciar todo:

```bash
docker compose up -d --scale cache_api=5
```

---

## Variables de entorno (`.env`)

```ini
# --- Caché (Redis) ---
REDIS_MAXMEMORY=200mb
REDIS_POLICY_NATIVE=allkeys-lru   # allkeys-lru / allkeys-lfu / noeviction
CACHE_POLICY=LRU                  # LRU / LFU / FIFO
CACHE_TTL_SEC=300

# TTL por tipo de consulta (segundos)
TTL_Q1=300
TTL_Q2=300
TTL_Q3=180
TTL_Q4=120
TTL_Q5=600

# --- Modo Kafka ---
USE_KAFKA=true        # true = asíncrono con Kafka | false = síncrono HTTP directo
MAX_RETRIES=3         # intentos antes de derivar a DLQ
RETRY_DELAY_SEC=0.1   # tiempo entre reintentos

# --- Latencia simulada del Generador de Respuestas (ms) ---
SIM_LATENCY_MIN_MS=30
SIM_LATENCY_MAX_MS=120
```

---

## Endpoints HTTP

### Generador de Tráfico (`localhost:5003`)
- `POST /run` — inicia un experimento, parámetros: `distribution`, `rate_qps`, `duration_sec`, `concurrency`, `seed`
- `POST /stop` — detiene el experimento en curso
- `GET /status` — estado actual: QPS, errores, hit rate de la ventana reciente

### Generador de Respuestas (`localhost:5001`)
- `POST /query` — resuelve una consulta de forma síncrona
- `POST /toggle_failure` — `{"enabled": true|false}` para simular caída del servicio

### Métricas (`localhost:5002`)
- `GET /summary` — resumen completo: latencias p50/p95, throughput, hit rate, lag Kafka
- `POST /snapshot` — guarda el estado actual con un label
- `POST /reset` — limpia todos los acumuladores de estadísticas
