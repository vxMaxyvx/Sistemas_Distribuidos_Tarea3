# Tarea 3 — Sistemas Distribuidos 2026-1

**Integrantes:** Vicente Cataldo, Maximiliano Oliva

Esta tarea extiende lo hecho en la Tarea 2 agregando un pipeline de observabilidad en tiempo real. La idea es que mientras el sistema procesa consultas geoespaciales con cache y Kafka, un flujo paralelo toma las metricas, las agrega con Spark y las muestra en dashboards de Kibana.

Resumidamente:

- El servicio de Metricas publica cada evento en un topico Kafka llamado `metrics-topic`.
- Spark Structured Streaming lee ese topico, calcula agregaciones por ventanas de tiempo y las guarda en Elasticsearch.
- Kibana muestra dashboards interactivos con throughput, latencias, hit rate y retry rate.

Todo corre en Docker Compose. No hay que instalar nada mas que Docker.

---

## Requisitos

- Docker Engine v24+ y Docker Compose v2
- Python 3.10+ con numpy (solo para generar el dataset una vez)

Verifica que tengas Docker:

```bash
docker --version
docker compose version
```

---

## Como levantar todo

### Paso 1: generar el dataset (solo la primera vez)

```bash
pip install numpy
python3 scripts/download_data.py
```

Esto crea `data/buildings_rm.csv` con edificaciones sinteticas de Santiago.

### Paso 2: levantar el stack

```bash
export REDIS_PORT_HOST=6380   # evita conflicto con Redis local
export USE_KAFKA=true
docker compose up -d --build
```

Eso levanta todos los servicios: Redis, Kafka, los 4 servicios de la Tarea 2, mas Elasticsearch, Kibana y Spark.

La primera vez tarda unos minutos porque Spark descarga los conectores de Kafka y Elasticsearch.

### Paso 3: configurar Kibana

```bash
bash kibana/setup.sh
```

Este script crea el indice en Elasticsearch, el data view en Kibana e importa el dashboard.

### Paso 4: generar trafico y ver el dashboard

```bash
curl -s -X POST http://localhost:5003/run \
  -H "Content-Type: application/json" \
  -d '{"distribution":"zipf","rate_qps":100,"duration_sec":300,"zipf_s":1.2,"concurrency":33,"seed":42,"label":"demo"}'
```

Despues abri Kibana en http://localhost:5601 y anda a Dashboards -> "Tarea 3 - Monitoreo en Tiempo Real". Pone el zoom temporal en "Last 15 minutes" y activa el auto-refresh cada 10 segundos.

---

## Como bajar todo

```bash
docker compose down
```

Si queres borrar tambien los volumenes (datos de Elasticsearch):

```bash
docker compose down -v
```

---

## Estructura del proyecto

```
.
├── cache_api/               # Servicio de cache y consumidor Kafka
├── generador_respuestas/    # Resuelve consultas Q1-Q5
├── generador_trafico/       # Genera trafico de consultas
├── metricas/                # Acumula metricas y publica a Kafka
├── spark_streaming/         # Job de Spark (lee metrics-topic -> Elasticsearch)
├── kibana/                  # Dashboard y script de setup
├── experiments/             # Scripts para correr escenarios automaticamente
├── scripts/                 # Genera el dataset
├── data/                    # Dataset de edificaciones
├── informe/                 # Informe de analisis
└── docker-compose.yml
```

---

## Servicios y puertos

| Servicio | Puerto | URL |
|---|---|---|
| Elasticsearch | 9200 | http://localhost:9200 |
| Kibana | 5601 | http://localhost:5601 |
| Generador de Trafico | 5003 | http://localhost:5003 |
| Metricas | 5002 | http://localhost:5002 |
| Generador de Respuestas | 5001 | http://localhost:5001 |
| Kafka | 9092 | kafka:9092 (interno) |
| Redis | 6380 (host) | redis:6379 (interno) |

---

## Como correr un escenario y verlo en Kibana

Los escenarios de la Tarea 2 se pueden correr manualmente o con el script automatico. El pipeline de metricas se alimenta solo, porque el servicio de Metricas publica cada evento en `metrics-topic` sin importar como se corra el trafico.

### Opcion A: automatico (8 escenarios seguidos)

```bash
python experiments/run_kafka_experiments.py
```

Tarda unos 25-35 minutos y guarda los resultados en `results/`.

### Opcion B: manual (un escenario a la vez)

#### Operacion normal (Kafka, 1 consumidor)

```bash
docker compose down
export USE_KAFKA=true
docker compose up -d --build

sleep 15

curl -s -X POST http://localhost:5002/reset
docker compose exec redis redis-cli FLUSHDB

curl -s -X POST http://localhost:5003/run \
  -H "Content-Type: application/json" \
  -d '{"distribution":"zipf","rate_qps":100,"duration_sec":120,"zipf_s":1.2,"concurrency":33,"seed":42,"label":"normal"}'
```

Espera a que termine (revisa `curl -s http://localhost:5003/status` hasta que diga `"running": false`) y ahi anda a Kibana a ver el dashboard.

#### Falla temporal del Generador de Respuestas

Mismo setup que arriba, pero mientras corre el trafico abri otra terminal:

```bash
# A los 30 segundos de iniciado el trafico
curl -s -X POST http://localhost:5001/toggle_failure \
  -H "Content-Type: application/json" -d '{"enabled": true}'
docker compose exec redis redis-cli FLUSHDB

# A los 60 segundos (30 segundos despues), restaurala
curl -s -X POST http://localhost:5001/toggle_failure \
  -H "Content-Type: application/json" -d '{"enabled": false}'
```

En Kibana vas a ver como cae el throughput, sube el retry rate y aparece dlq rate durante la falla.

#### Multiples consumidores

```bash
docker compose up -d --build --scale cache_api=3
```

Y despues lanza el trafico igual que en operacion normal. En Kibana se ve como mejora el throughput y baja la latencia al repartir la carga.

#### Spike de trafico

Lanza trafico normal, despues un pico de 4x la tasa, y despues de nuevo normal. En Kibana se ve el pico en throughput y latencia, y como se normaliza despues.

---

## Verificar que todo funciona

Si queres chequear que el pipeline esta andando:

```bash
# Hay documentos en Elasticsearch?
curl -s "http://localhost:9200/metrics-aggregated/_count"

# Spark esta procesando?
docker compose logs -f spark

# Llegan eventos a Kafka?
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic metrics-topic --max-messages 3
```

---

## Troubleshooting basico

- **Dashboard vacio:** asegurate de haber corrido `bash kibana/setup.sh` y de que haya trafico generandose. Spark tarda unos 40-60 segundos en escribir la primera ventana.
- **Elasticsearch no levanta:** en Linux puede necesitar `sudo sysctl -w vm.max_map_count=262144`.
- **Spark no arranca:** la primera vez descarga jars de internet. Si no tenes conexion o es lenta, va a fallar.

---

## Variables de entorno importantes

Estas van en un archivo `.env` o las exportas antes de `docker compose up`:

```ini
USE_KAFKA=true              # true = modo asincrono con Kafka
PUBLISH_METRICS_KAFKA=true  # publicar metricas en metrics-topic
REDIS_PORT_HOST=6380        # puerto de Redis en el host (cambiar si 6379 esta ocupado)
```
