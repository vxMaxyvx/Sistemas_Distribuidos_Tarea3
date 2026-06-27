"""
Cliente de cache Redis con soporte para politicas LRU, LFU y FIFO.
FIFO se implementa a nivel de cliente usando una lista ordenada en Redis
con politica 'noeviction', realizando eviccion manual cuando se supera
la memoria maxima.
"""
import os
import json
import time
import logging
from typing import Optional
import redis

log = logging.getLogger("cache")

POLICY = os.getenv("CACHE_POLICY", "LRU").upper()
DEFAULT_TTL = int(os.getenv("CACHE_TTL_SEC", "300"))
FIFO_CHECK_EVERY = int(os.getenv("FIFO_CHECK_EVERY", "50"))
FIFO_ORDER_KEY = "__fifo_order__"


class CacheClient:
    """Cliente de cache Redis con soporte LRU, LFU y FIFO."""

    def __init__(self, host: str, port: int, db: int = 0):
        self.r = redis.Redis(host=host, port=port, db=db,
                             decode_responses=True)
        self.policy = POLICY
        self._fifo_counter = 0
        self._wait_for_redis()
        self._configure_policy()

    def _wait_for_redis(self, timeout: int = 30):
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                self.r.ping()
                log.info("Redis conectado")
                return
            except redis.ConnectionError:
                time.sleep(0.5)
        raise RuntimeError("Redis no respondio a tiempo")

    def _configure_policy(self):
        """Configura la politica de eviccion en Redis."""
        if self.policy in ("LRU", "LFU"):
            policy_str = "allkeys-lru" if self.policy == "LRU" else "allkeys-lfu"
            try:
                self.r.config_set("maxmemory-policy", policy_str)
                log.info(f"Politica Redis configurada: {policy_str}")
            except redis.ResponseError as e:
                log.warning(f"No se pudo configurar politica ({e}); "
                            f"asumiendo set en docker")
        elif self.policy == "FIFO":
            try:
                self.r.config_set("maxmemory-policy", "noeviction")
                log.info("Politica Redis: noeviction (FIFO manejado por cliente)")
            except redis.ResponseError as e:
                log.warning(f"No se pudo configurar politica: {e}")
        else:
            raise ValueError(f"Politica desconocida: {self.policy}")

    def get(self, key: str) -> Optional[dict]:
        """Obtiene un valor del cache."""
        raw = self.r.get(key)
        if raw is None:
            return None
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            log.error(f"Valor corrupto en key {key}")
            return None

    def set(self, key: str, value: dict, ttl: Optional[int] = None) -> bool:
        """Guarda un valor en el cache con TTL."""
        ttl_to_use = ttl if ttl is not None else DEFAULT_TTL
        payload = json.dumps(value, separators=(",", ":"))

        if ttl_to_use > 0:
            self.r.set(key, payload, ex=ttl_to_use)
        else:
            self.r.set(key, payload)

        if self.policy == "FIFO":
            self.r.rpush(FIFO_ORDER_KEY, key)
            self._fifo_counter += 1
            if self._fifo_counter % FIFO_CHECK_EVERY == 0:
                self._fifo_evict_if_needed()
        return True

    def _fifo_evict_if_needed(self):
        """Eviccion manual FIFO: elimina las keys mas antiguas."""
        try:
            info = self.r.info("memory")
            used = int(info.get("used_memory", 0))
            maxmem = int(info.get("maxmemory", 0) or 0)
        except Exception as e:
            log.warning(f"FIFO: no pude leer info memory: {e}")
            return

        if maxmem == 0 or used <= maxmem:
            return

        evicted = 0
        while True:
            try:
                info = self.r.info("memory")
                used = int(info.get("used_memory", 0))
            except Exception:
                break
            if used <= maxmem * 0.95:
                break

            old_key = self.r.lpop(FIFO_ORDER_KEY)
            if old_key is None:
                break
            n = self.r.delete(old_key)
            if n > 0:
                evicted += 1
                self.r.incr("__fifo_evictions__")
            if evicted > 1000:
                break

        if evicted > 0:
            log.info(f"FIFO: evicted {evicted} keys")

    def stats(self) -> dict:
        """Stats agregados de Redis para metricas."""
        info = self.r.info()
        result = {
            "policy": self.policy,
            "used_memory": int(info.get("used_memory", 0)),
            "used_memory_human": info.get("used_memory_human", "0"),
            "maxmemory": int(info.get("maxmemory", 0) or 0),
            "n_keys": self.r.dbsize() - (1 if self.policy == "FIFO" else 0),
            "evicted_keys": int(info.get("evicted_keys", 0)),
            "keyspace_hits": int(info.get("keyspace_hits", 0)),
            "keyspace_misses": int(info.get("keyspace_misses", 0)),
        }
        if self.policy == "FIFO":
            try:
                manual = int(self.r.get("__fifo_evictions__") or 0)
                result["evicted_keys"] = manual
            except Exception:
                pass
        return result

    def flushall(self):
        """Limpia todo el cache."""
        self.r.flushdb()
