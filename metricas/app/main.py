"""
Servicio de Almacenamiento de Metricas.
Recibe eventos del sistema de cache via HTTP y los mantiene en memoria (RAM).
Calcula hit rate, throughput, latencias p50/p95/p99, eviction rate y
cache efficiency. Soporta desglose por tipo de consulta Q1-Q5 y snapshots.
"""
import os
import time
import json
import logging
import threading
from collections import defaultdict, deque
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import httpx
from fastapi import FastAPI
from pydantic import BaseModel, Field

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [metricas] %(message)s")
log = logging.getLogger(__name__)

LATENCY_WINDOW = int(os.getenv("LATENCY_WINDOW", "5000"))
SNAPSHOT_DIR = Path(os.getenv("SNAPSHOT_DIR", "/snapshots"))
CACHE_STATS_URL = os.getenv("CACHE_STATS_URL",
                            "http://cache_api:5000/stats")


USE_KAFKA = os.getenv("USE_KAFKA", "false").lower() == "true"
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

# Tarea 3: publicar eventos en metrics-topic para que Spark los procese.
PUBLISH_METRICS_KAFKA = os.getenv("PUBLISH_METRICS_KAFKA", "true").lower() == "true"
METRICS_TOPIC = os.getenv("METRICS_TOPIC", "metrics-topic")
METRICS_TOPIC_PARTITIONS = int(os.getenv("METRICS_TOPIC_PARTITIONS", "3"))


class MetricsPublisher:
    """Publica eventos en metrics-topic. Si Kafka no esta, no se cae el sistema."""

    def __init__(self):
        self.producer = None
        self.enabled = PUBLISH_METRICS_KAFKA
        self._lock = threading.Lock()

    def start(self):
        if not self.enabled:
            log.info("Publicacion de metricas en Kafka deshabilitada")
            return
        thread = threading.Thread(target=self._connect, daemon=True)
        thread.start()

    def _connect(self):
        from kafka import KafkaProducer
        from kafka.admin import KafkaAdminClient, NewTopic

        # Crear el topico dedicado de metricas si no existe.
        for attempt in range(20):
            try:
                admin = KafkaAdminClient(
                    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                    client_id="metricas-admin",
                )
                existing = admin.list_topics()
                if METRICS_TOPIC not in existing:
                    admin.create_topics([NewTopic(
                        name=METRICS_TOPIC,
                        num_partitions=METRICS_TOPIC_PARTITIONS,
                        replication_factor=1,
                    )])
                    log.info(f"Topico de metricas creado: {METRICS_TOPIC}")
                admin.close()
                break
            except Exception as e:
                log.warning(f"Esperando Kafka para crear {METRICS_TOPIC} "
                            f"(intento {attempt + 1}/20): {e}")
                time.sleep(2.0)

        # Crear el productor.
        for attempt in range(20):
            try:
                producer = KafkaProducer(
                    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    linger_ms=50,
                    acks=1,
                )
                with self._lock:
                    self.producer = producer
                log.info(f"Publicador de metricas Kafka listo -> {METRICS_TOPIC}")
                return
            except Exception as e:
                log.warning(f"Esperando Kafka para el productor de metricas "
                            f"(intento {attempt + 1}/20): {e}")
                time.sleep(2.0)
        log.error("No se pudo inicializar el publicador de metricas en Kafka")

    def publish(self, event: dict):
        """Construye el evento estructurado y lo envia a metrics-topic."""
        if not self.enabled or self.producer is None:
            return
        try:
            self.producer.send(METRICS_TOPIC, value=_to_metric_event(event))
        except Exception as e:
            log.debug(f"No se pudo publicar metrica en Kafka: {e}")

    def close(self):
        if self.producer is not None:
            try:
                self.producer.flush(timeout=5)
                self.producer.close()
            except Exception:
                pass


def _to_metric_event(ev: dict) -> dict:
    """Normaliza un evento al esquema que espera Spark."""
    etype = ev.get("event")
    if etype in ("hit", "miss", "recovery"):
        status = "success"
    elif etype in ("dlq", "error"):
        status = "failed"
    else:  # retry
        status = "pending"

    if etype == "hit":
        cache_hit = True
    elif etype in ("miss", "recovery"):
        cache_hit = False
    else:
        cache_hit = None

    ts = float(ev.get("ts") or time.time())
    iso = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()

    return {
        "ts": ts,
        "timestamp": iso,
        "event_type": etype,
        "query_type": (ev.get("query_type") or "UNK").upper(),
        "latency_ms": ev.get("latency_ms"),
        "cache_hit": cache_hit,
        # `recovery` es una consulta que se resolvio despues de >=1 reintento.
        "was_retried": etype == "recovery",
        "is_retry_event": etype == "retry",
        "retry_count": ev.get("retry_count") if ev.get("retry_count") is not None else (1 if etype == "retry" else (0 if etype != "dlq" else None)),
        "status": status,
        "key": ev.get("key"),
    }


publisher = MetricsPublisher()


def get_kafka_backlog() -> int:
    """Calcula el backlog (lag) total de la cola principal y de reintentos."""
    if not USE_KAFKA:
        return 0
    try:
        from kafka import TopicPartition
        from kafka.admin import KafkaAdminClient

        admin = KafkaAdminClient(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            request_timeout_ms=3000,
        )
        # Obtener offsets committed del grupo "cache-group"
        group_offsets = admin.list_consumer_group_offsets("cache-group")

        # Crear un consumer ligero SIN grupo para consultar end offsets
        from kafka import KafkaConsumer
        inspector = KafkaConsumer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            consumer_timeout_ms=1000,
        )

        backlog = 0
        for topic in ["queries", "retry-queries"]:
            partitions = inspector.partitions_for_topic(topic)
            if not partitions:
                continue
            tps = [TopicPartition(topic, p) for p in partitions]
            end_offsets = inspector.end_offsets(tps)
            for tp in tps:
                committed = 0
                if tp in group_offsets:
                    committed = group_offsets[tp].offset
                latest = end_offsets.get(tp, 0)
                lag = max(0, latest - committed)
                backlog += lag

        inspector.close()
        admin.close()
        return backlog
    except Exception as e:
        log.warning(f"Error consultando backlog de Kafka: {e}")
        return 0


class Metrics:
    """Recolector de metricas del sistema de cache extendido para Kafka (Tarea 2)."""

    def __init__(self):
        self.lock = threading.Lock()
        self.start_time = time.time()
        self._reset_counters()

    def _reset_counters(self):
        self.hits_total = 0
        self.misses_total = 0
        self.recoveries_total = 0
        self.retries_total = 0
        self.dlq_total = 0
        self.errors_total = 0

        # Por tipo de consulta
        self.hits_by_q: dict[str, int] = defaultdict(int)
        self.misses_by_q: dict[str, int] = defaultdict(int)
        self.recoveries_by_q: dict[str, int] = defaultdict(int)
        self.retries_by_q: dict[str, int] = defaultdict(int)
        self.dlq_by_q: dict[str, int] = defaultdict(int)

        # Latencias por evento
        self.latencies_hit: deque[float] = deque(maxlen=LATENCY_WINDOW)
        self.latencies_miss: deque[float] = deque(maxlen=LATENCY_WINDOW)
        self.latencies_recovery: deque[float] = deque(maxlen=LATENCY_WINDOW)
        self.latencies_by_q: dict[str, deque] = defaultdict(
            lambda: deque(maxlen=LATENCY_WINDOW))

        # Throughputs
        self.event_times: deque[float] = deque(maxlen=20000)

        # Eviction rate
        self.last_eviction_snapshot: int = 0
        self.last_eviction_time: float = self.start_time

    def reset(self):
        with self.lock:
            self.start_time = time.time()
            self._reset_counters()

    def record(self, event: dict):
        """Registra un evento de cache o Kafka (hit, miss, recovery, retry, dlq, error)."""
        ev = event.get("event")
        qt = (event.get("query_type") or "UNK").upper()
        latency = float(event.get("latency_ms") or 0)
        ts = float(event.get("ts") or time.time())

        with self.lock:
            if ev == "hit":
                self.hits_total += 1
                self.hits_by_q[qt] += 1
                self.latencies_hit.append(latency)
                self.latencies_by_q[qt].append(latency)
                self.event_times.append(ts)
            elif ev == "miss":
                self.misses_total += 1
                self.misses_by_q[qt] += 1
                self.latencies_miss.append(latency)
                self.latencies_by_q[qt].append(latency)
                self.event_times.append(ts)
            elif ev == "recovery":
                self.recoveries_total += 1
                self.recoveries_by_q[qt] += 1
                self.latencies_recovery.append(latency)
                self.latencies_by_q[qt].append(latency)
                self.event_times.append(ts)
            elif ev == "retry":
                self.retries_total += 1
                self.retries_by_q[qt] += 1
            elif ev == "dlq":
                self.dlq_total += 1
                self.dlq_by_q[qt] += 1
            elif ev == "error":
                self.errors_total += 1

    def summary(self, cache_stats: dict | None = None) -> dict:
        """Resumen global de metricas."""
        with self.lock:
            # Total de consultas exitosas
            total = self.hits_total + self.misses_total + self.recoveries_total
            elapsed = time.time() - self.start_time

            hit_rate = (self.hits_total / total) if total > 0 else None
            miss_rate = (self.misses_total / total) if total > 0 else None
            
            # Tasas de Kafka
            total_attempts = total + self.dlq_total
            retry_rate = self.retries_total / total_attempts if total_attempts > 0 else 0.0
            recovery_rate = self.recoveries_total / (self.recoveries_total + self.dlq_total) \
                if (self.recoveries_total + self.dlq_total) > 0 else 0.0
            dlq_rate = self.dlq_total / total_attempts if total_attempts > 0 else 0.0

            # Throughput total y reciente
            throughput = total / elapsed if elapsed > 0 else 0
            now = time.time()
            recent = [t for t in self.event_times if t >= now - 10]
            throughput_recent = len(recent) / 10.0 if recent else 0

            # Throughput de procesamiento (ventana entre primer y ultimo evento)
            if len(self.event_times) >= 2:
                processing_window = self.event_times[-1] - self.event_times[0]
                throughput_processing = total / processing_window if processing_window > 0 else 0
            else:
                throughput_processing = throughput

            # Calculo de percentiles
            def percentiles(arr: deque, ps=(50, 95, 99)):
                if not arr:
                    return {f"p{p}": None for p in ps}
                a = np.array(arr)
                return {f"p{p}": round(float(np.percentile(a, p)), 3)
                        for p in ps}

            lat_hit = percentiles(self.latencies_hit)
            lat_miss = percentiles(self.latencies_miss)
            lat_rec = percentiles(self.latencies_recovery)
            lat_all = percentiles(
                deque(list(self.latencies_hit) + list(self.latencies_miss) + list(self.latencies_recovery))
            )

            # Cache efficiency
            mean_t_cache = float(np.mean(self.latencies_hit)) \
                if self.latencies_hit else 0
            mean_t_db = float(np.mean(list(self.latencies_miss) + list(self.latencies_recovery))) \
                if (self.latencies_miss or self.latencies_recovery) else 0

            efficiency = None
            if total > 0 and mean_t_db > 0:
                saved = self.hits_total * (mean_t_db - mean_t_cache)
                efficiency = round(saved / total, 3)

            # Eviction rate
            eviction_rate_per_min = None
            current_evicted = None
            if cache_stats:
                current_evicted = int(cache_stats.get("evicted_keys", 0))
                dt = now - self.last_eviction_time
                if dt > 0:
                    delta = current_evicted - self.last_eviction_snapshot
                    eviction_rate_per_min = round(delta * 60.0 / dt, 2)

            # Obtener backlog real en Kafka
            backlog_size = get_kafka_backlog()

            return {
                "elapsed_sec": round(elapsed, 2),
                "totals": {
                    "hits": self.hits_total,
                    "misses": self.misses_total,
                    "recoveries": self.recoveries_total,
                    "retries": self.retries_total,
                    "dlq": self.dlq_total,
                    "errors": self.errors_total,
                    "total_requests": total,
                },
                "hit_rate": round(hit_rate, 4) if hit_rate is not None else None,
                "miss_rate": round(miss_rate, 4) if miss_rate is not None else None,
                "retry_rate": round(retry_rate, 4),
                "recovery_rate": round(recovery_rate, 4),
                "dlq_rate": round(dlq_rate, 4),
                "backlog_size": backlog_size,
                "throughput_qps_total": round(throughput_processing, 2),
                "throughput_qps_recent_10s": round(throughput_recent, 2),
                "latency_ms_hit": lat_hit,
                "latency_ms_miss": lat_miss,
                "latency_ms_recovery": lat_rec,
                "latency_ms_all": lat_all,
                "mean_t_cache_ms": round(mean_t_cache, 3),
                "mean_t_db_ms": round(mean_t_db, 3),
                "cache_efficiency": efficiency,
                "eviction": {
                    "total_evicted": current_evicted,
                    "rate_per_min": eviction_rate_per_min,
                },
                "cache_redis_stats": cache_stats,
            }

    def update_eviction_marker(self, current_evicted: int):
        with self.lock:
            self.last_eviction_snapshot = current_evicted
            self.last_eviction_time = time.time()

    def by_query_summary(self) -> dict:
        """Desglose de metricas por tipo de consulta Q1-Q5."""
        with self.lock:
            out = {}
            for qt in ("Q1", "Q2", "Q3", "Q4", "Q5"):
                h = self.hits_by_q.get(qt, 0)
                m = self.misses_by_q.get(qt, 0)
                rec = self.recoveries_by_q.get(qt, 0)
                ret = self.retries_by_q.get(qt, 0)
                dlq = self.dlq_by_q.get(qt, 0)
                tot = h + m + rec
                lats = self.latencies_by_q.get(qt, deque())
                if lats:
                    a = np.array(lats)
                    p50 = float(np.percentile(a, 50))
                    p95 = float(np.percentile(a, 95))
                    p99 = float(np.percentile(a, 99))
                else:
                    p50 = p95 = p99 = None
                out[qt] = {
                    "hits": h, "misses": m, "recoveries": rec,
                    "retries": ret, "dlq": dlq, "total": tot,
                    "hit_rate": round(h / tot, 4) if tot else None,
                    "p50_ms": round(p50, 3) if p50 is not None else None,
                    "p95_ms": round(p95, 3) if p95 is not None else None,
                    "p99_ms": round(p99, 3) if p99 is not None else None,
                }
            return out



metrics = Metrics()
http: httpx.AsyncClient | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global http
    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
    http = httpx.AsyncClient(timeout=5.0)
    publisher.start()
    log.info("Servicio de Metricas listo")
    yield
    publisher.close()
    await http.aclose()


app = FastAPI(title="Servicio de Metricas", lifespan=lifespan)


class Event(BaseModel):
    event: str
    query_type: str | None = None
    key: str | None = None
    latency_ms: float | None = None
    lookup_ms: float | None = None
    compute_ms: float | None = None
    ttl: int | None = None
    error: str | None = None
    retry_count: int | None = None
    ts: float | None = None


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/event")
async def event(ev: Event):
    data = ev.dict()
    # Plano de procesamiento: acumulado en memoria (Tarea 1 y 2).
    metrics.record(data)
    # Plano de observabilidad (Tarea 3): publicar en metrics-topic para Spark.
    publisher.publish(data)
    return {"ok": True}


async def _fetch_cache_stats() -> dict | None:
    """Obtiene stats del cache service."""
    if http is None:
        return None
    try:
        r = await http.get(CACHE_STATS_URL, timeout=2.0)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.debug(f"No pude obtener stats del cache: {e}")
        return None


@app.get("/summary")
async def summary():
    cache_stats = await _fetch_cache_stats()
    s = metrics.summary(cache_stats)
    if cache_stats:
        metrics.update_eviction_marker(
            int(cache_stats.get("evicted_keys", 0)))
    return s


@app.get("/summary/by_query")
async def by_query():
    return metrics.by_query_summary()


@app.post("/reset")
async def reset():
    metrics.reset()
    log.info("Metricas reiniciadas")
    return {"status": "reset"}


class SnapshotRequest(BaseModel):
    label: str = Field("snapshot",
                       description="Nombre descriptivo del experimento")
    extra: dict[str, Any] = Field(default_factory=dict)


@app.post("/snapshot")
async def snapshot(req: SnapshotRequest):
    cache_stats = await _fetch_cache_stats()
    summary_data = metrics.summary(cache_stats)
    summary_data["by_query"] = metrics.by_query_summary()
    summary_data["label"] = req.label
    summary_data["extra"] = req.extra
    summary_data["snapshot_ts"] = time.time()

    safe_label = req.label.replace("/", "_").replace(" ", "_")
    fname = f"{int(time.time())}_{safe_label}.json"
    path = SNAPSHOT_DIR / fname
    with open(path, "w") as f:
        json.dump(summary_data, f, indent=2, default=str)
    log.info(f"Snapshot guardado: {path}")
    return {"path": str(path), "summary": summary_data}
