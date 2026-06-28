"""
Servicio de Cache - Intercepta consultas y las resuelve usando Redis.
Si hay cache hit retorna directamente; si no, delega al Generador de Respuestas.
Registra todas las metricas enviandolas via HTTP al servicio de Metricas.
Soporta politicas LRU, LFU y FIFO con TTL configurable por tipo de consulta.
"""
import os
import time
import logging
import asyncio
import threading
import json
from contextlib import asynccontextmanager
from typing import Any
from concurrent.futures import ThreadPoolExecutor
import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from kafka import KafkaConsumer, KafkaProducer

from .cache import CacheClient

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [cache-svc] %(message)s")
log = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

# Variables de entorno
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
RESPONSE_GEN_URL = os.getenv("RESPONSE_GEN_URL",
                             "http://generador_respuestas:5001")
METRICAS_URL = os.getenv("METRICAS_URL", "http://metricas:5002")
USE_KAFKA = os.getenv("USE_KAFKA", "false").lower() == "true"
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
RETRY_DELAY_SEC = float(os.getenv("RETRY_DELAY_SEC", "0.1"))

# TTL por tipo de consulta
TTL_BY_QUERY = {
    "Q1": int(os.getenv("TTL_Q1", "300")),
    "Q2": int(os.getenv("TTL_Q2", "300")),
    "Q3": int(os.getenv("TTL_Q3", "180")),
    "Q4": int(os.getenv("TTL_Q4", "120")),
    "Q5": int(os.getenv("TTL_Q5", "600")),
}

# Clientes globales
cache: CacheClient | None = None
http: httpx.AsyncClient | None = None
consumer_thread: threading.Thread | None = None


def _build_cache_key(query_type: str, params: dict[str, Any]) -> str:
    """Genera la cache key segun el formato del enunciado."""
    qt = query_type.upper()
    if qt == "Q1":
        return f"count:{params['zone_id']}:conf={params.get('confidence_min', 0.0):.2f}"
    if qt == "Q2":
        return f"area:{params['zone_id']}:conf={params.get('confidence_min', 0.0):.2f}"
    if qt == "Q3":
        return f"density:{params['zone_id']}:conf={params.get('confidence_min', 0.0):.2f}"
    if qt == "Q4":
        return (
            f"compare:density:{params['zone_a']}:{params['zone_b']}"
            f":conf={params.get('confidence_min', 0.0):.2f}"
        )
    if qt == "Q5":
        return f"confidence_dist:{params['zone_id']}:bins={int(params.get('bins', 5))}"
    raise ValueError(f"Tipo de consulta desconocido: {query_type}")


CONSUMER_CONCURRENCY = int(os.getenv("CONSUMER_CONCURRENCY", "32"))


def _process_single_message(query, topic, sync_http, producer):
    """Procesa un unico mensaje de Kafka (ejecutado en thread pool)."""
    query_id = query.get("query_id")
    query_type = query.get("query_type")
    params = query.get("params", {})
    retry_count = query.get("retry_count", 0)
    created_at = query.get("created_at", time.time())

    try:
        key = _build_cache_key(query_type, params)
    except Exception as e:
        log.error(f"Error construyendo cache key para {query_id}: {e}")
        return

    # Buscar en Redis
    t_lookup_start = time.perf_counter()
    cached = cache.get(key)
    t_lookup_ms = (time.perf_counter() - t_lookup_start) * 1000

    # 1. CACHE HIT
    if cached is not None:
        latency_ms = (time.time() - created_at) * 1000
        event = "recovery" if retry_count > 0 else "hit"
        try:
            sync_http.post(f"{METRICAS_URL}/event", json={
                "event": event,
                "query_type": query_type.upper(),
                "key": key,
                "latency_ms": latency_ms,
                "lookup_ms": t_lookup_ms,
                "retry_count": retry_count,
                "ts": time.time(),
            }, timeout=2.0)
        except Exception:
            pass
        return

    # 2. CACHE MISS - Llamar al Generador de Respuestas
    try:
        resp = sync_http.post(
            f"{RESPONSE_GEN_URL}/query",
            json={"query_type": query_type, "params": params},
            timeout=5.0
        )
        resp.raise_for_status()
        data = resp.json()
        result = data["result"]
        compute_ms = data["compute_time_ms"]

        # Guardar en cache
        ttl = TTL_BY_QUERY.get(query_type.upper(), 300)
        cache.set(key, result, ttl=ttl)

        latency_ms = (time.time() - created_at) * 1000
        event = "recovery" if retry_count > 0 else "miss"

        try:
            sync_http.post(f"{METRICAS_URL}/event", json={
                "event": event,
                "query_type": query_type.upper(),
                "key": key,
                "latency_ms": latency_ms,
                "lookup_ms": t_lookup_ms,
                "compute_ms": compute_ms,
                "ttl": ttl,
                "retry_count": retry_count,
                "ts": time.time(),
            }, timeout=2.0)
        except Exception:
            pass

    except Exception as e:
        # FALLA TEMPORAL: Reintentar o DLQ
        log.warning(f"Error resolviendo consulta {query_id} (intento {retry_count}): {e}")

        if retry_count < MAX_RETRIES:
            new_retry_count = retry_count + 1
            retry_payload = {
                "query_id": query_id,
                "query_type": query_type,
                "params": params,
                "retry_count": new_retry_count,
                "created_at": created_at,
            }

            # Registrar metrica de reintento PRIMERO (siempre)
            try:
                sync_http.post(f"{METRICAS_URL}/event", json={
                    "event": "retry",
                    "query_type": query_type.upper(),
                    "key": key,
                    "error": str(e),
                    "retry_count": retry_count,
                    "ts": time.time(),
                }, timeout=2.0)
            except Exception:
                pass

            # Enviar a cola de reintentos
            time.sleep(RETRY_DELAY_SEC)
            try:
                producer.send("retry-queries", value=retry_payload)
                producer.flush()
            except Exception as pe:
                log.error(f"Error enviando retry a Kafka: {pe}")
        else:
            # Registrar metrica DLQ PRIMERO
            try:
                sync_http.post(f"{METRICAS_URL}/event", json={
                    "event": "dlq",
                    "query_type": query_type.upper(),
                    "key": key,
                    "error": str(e),
                    "retry_count": retry_count,
                    "ts": time.time(),
                }, timeout=2.0)
            except Exception:
                pass

            # Enviar a DLQ
            dlq_payload = {
                "query_id": query_id,
                "query_type": query_type,
                "params": params,
                "retry_count": retry_count,
                "created_at": created_at,
                "failed_at": time.time(),
                "error": str(e)
            }
            try:
                producer.send("dlq-queries", value=dlq_payload)
                producer.flush()
            except Exception as pe:
                log.error(f"Error enviando DLQ a Kafka: {pe}")
            log.error(f"Consulta {query_id} enviada a la DLQ tras {retry_count} reintentos")


def _ensure_topics():
    """Crea topicos Kafka con 3 particiones si no existen, antes de iniciar consumer."""
    from kafka.admin import KafkaAdminClient, NewTopic, NewPartitions
    try:
        admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
        existing = admin.list_topics()
        desired = 3
        new_topics = []
        for t in ["queries", "retry-queries", "dlq-queries"]:
            if t not in existing:
                new_topics.append(NewTopic(name=t, num_partitions=desired, replication_factor=1))
        if new_topics:
            admin.create_topics(new_topics)
            log.info(f"Topicos creados: {[t.name for t in new_topics]}")

        # Aumentar particiones si necesario
        topic_meta = admin.describe_topics([t for t in ["queries", "retry-queries"] if t in existing])
        to_increase = {}
        for meta in topic_meta:
            if len(meta.get("partitions", [])) < desired:
                to_increase[meta["topic"]] = NewPartitions(total_count=desired)
        if to_increase:
            admin.create_partitions(to_increase)
            log.info(f"Particiones aumentadas a {desired}: {list(to_increase.keys())}")
        admin.close()
    except Exception as e:
        log.warning(f"Error creando topicos: {e}")


def run_kafka_consumer():
    """Loop consumidor de Kafka con procesamiento concurrente via ThreadPool."""
    log.info("Iniciando hilo del consumidor Kafka...")

    # Asegurar que los topicos existan con particiones correctas
    _ensure_topics()

    # Intentar conectar el consumidor
    consumer = None
    for attempt in range(15):
        try:
            consumer = KafkaConsumer(
                "queries", "retry-queries",
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id="cache-group",
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                max_poll_records=50,
                auto_offset_reset="earliest",
                enable_auto_commit=False,
            )
            log.info("Consumidor Kafka conectado exitosamente!")
            break
        except Exception as e:
            log.warning(f"Consumidor esperando Kafka (intento {attempt + 1}/15): {e}")
            time.sleep(2.0)
    else:
        log.error("No se pudo iniciar el consumidor Kafka. Hilo abortado.")
        return

    # Productor para reintentos y DLQ
    producer = None
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        log.info("Productor Kafka del consumidor inicializado!")
    except Exception as e:
        log.error(f"No se pudo inicializar productor Kafka en consumidor: {e}")
        consumer.close()
        return

    sync_http = httpx.Client(timeout=10.0)
    executor = ThreadPoolExecutor(max_workers=CONSUMER_CONCURRENCY)
    # Semaforo de back-pressure: limita tareas en vuelo sin bloquear el poll.
    # Esto es CRITICO: si esperamos fut.result() antes del proximo poll,
    # los threads durmiendo (RETRY_DELAY_SEC) paralizan el consumer y acumulan
    # un backlog tan grande que retry_count=3 se procesa DESPUES de la recuperacion
    # -> DLQ=0. Con semaforo no-bloqueante, los reintentos ciclan rapidamente.
    sem = threading.Semaphore(CONSUMER_CONCURRENCY * 4)

    def _guarded_process(query, topic):
        try:
            _process_single_message(query, topic, sync_http, producer)
        finally:
            sem.release()

    log.info(f"Consumidor Kafka con {CONSUMER_CONCURRENCY} workers (no-bloqueante, sem={CONSUMER_CONCURRENCY*4})")

    try:
        while True:
            records = consumer.poll(timeout_ms=500)
            if not records:
                continue

            for tp, messages in records.items():
                for msg in messages:
                    sem.acquire()  # back-pressure: bloquea SOLO si hay demasiados en vuelo
                    executor.submit(_guarded_process, msg.value, msg.topic)

            # Commit inmediato tras submitir (no esperar fin de procesamiento).
            # Permite que retries en retry-queues se procesen sin quedar detras
            # del backlog de la cola principal.
            consumer.commit()

    except Exception as e:
        log.error(f"Excepcion en loop principal del consumidor: {e}")
    finally:
        executor.shutdown(wait=False)
        consumer.close()
        producer.close()
        sync_http.close()
        log.info("Hilo del consumidor Kafka terminado.")


@asynccontextmanager
async def lifespan(app: FastAPI):
    global cache, http, consumer_thread
    cache = CacheClient(REDIS_HOST, REDIS_PORT)
    http = httpx.AsyncClient(timeout=30.0)
    log.info("Cache Service listo")

    if USE_KAFKA:
        consumer_thread = threading.Thread(target=run_kafka_consumer, daemon=True)
        consumer_thread.start()
        log.info("Hilo de consumidor Kafka iniciado como daemon.")

    yield
    await http.aclose()


app = FastAPI(title="Cache API", lifespan=lifespan)


class QueryRequest(BaseModel):
    query_type: str
    params: dict[str, Any] = Field(default_factory=dict)
    client_id: str | None = None


async def _send_metric(event: dict):
    """Envia un evento de metrica al servicio de metricas (fire-and-forget)."""
    try:
        await http.post(f"{METRICAS_URL}/event", json=event, timeout=2.0)
    except Exception as e:
        log.debug(f"Metrics post failed: {e}")


@app.get("/health")
async def health():
    return {"status": "ok",
            "policy": cache.policy if cache else None}


@app.get("/stats")
async def stats():
    if cache is None:
        raise HTTPException(503, "Cache no inicializado")
    return cache.stats()


@app.post("/flush")
async def flush():
    if cache is None:
        raise HTTPException(503, "Cache no inicializado")
    cache.flushall()
    return {"status": "flushed"}


@app.post("/query")
async def query(req: QueryRequest):
    if cache is None or http is None:
        raise HTTPException(503, "Servicio no inicializado")

    t_total_start = time.perf_counter()

    try:
        key = _build_cache_key(req.query_type, req.params)
    except (KeyError, ValueError) as e:
        raise HTTPException(400, f"Parametros invalidos: {e}")

    # Busqueda en cache
    t_lookup_start = time.perf_counter()
    cached = cache.get(key)
    t_lookup_ms = (time.perf_counter() - t_lookup_start) * 1000

    # CACHE HIT
    if cached is not None:
        latency_ms = (time.perf_counter() - t_total_start) * 1000

        asyncio.create_task(_send_metric({
            "event": "hit",
            "query_type": req.query_type.upper(),
            "key": key,
            "latency_ms": latency_ms,
            "lookup_ms": t_lookup_ms,
            "ts": time.time(),
        }))
        return {
            "result": cached,
            "cache": "HIT",
            "latency_ms": latency_ms,
            "key": key,
        }

    # CACHE MISS - delegar al generador de respuestas
    try:
        resp = await http.post(
            f"{RESPONSE_GEN_URL}/query",
            json={"query_type": req.query_type, "params": req.params},
            timeout=10.0,
        )
        resp.raise_for_status()
        data = resp.json()
    except httpx.HTTPError as e:
        asyncio.create_task(_send_metric({
            "event": "error",
            "query_type": req.query_type.upper(),
            "key": key,
            "error": str(e),
            "ts": time.time(),
        }))
        raise HTTPException(502, f"Generador de respuestas fallo: {e}")

    # Guardar en cache con TTL por tipo de consulta
    result = data["result"]
    compute_ms = data["compute_time_ms"]

    ttl = TTL_BY_QUERY.get(req.query_type.upper(), 300)
    cache.set(key, result, ttl=ttl)

    # Metricas del miss
    latency_ms = (time.perf_counter() - t_total_start) * 1000

    asyncio.create_task(_send_metric({
        "event": "miss",
        "query_type": req.query_type.upper(),
        "key": key,
        "latency_ms": latency_ms,
        "lookup_ms": t_lookup_ms,
        "compute_ms": compute_ms,
        "ttl": ttl,
        "ts": time.time(),
    }))

    return {
        "result": result,
        "cache": "MISS",
        "latency_ms": latency_ms,
        "compute_ms": compute_ms,
        "key": key,
        "ttl": ttl,
    }
