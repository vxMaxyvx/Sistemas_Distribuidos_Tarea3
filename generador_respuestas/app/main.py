"""
Generador de Respuestas - Servicio FastAPI que procesa consultas Q1-Q5
sobre el dataset de edificaciones de Santiago precargado en memoria.
"""
import os
import time
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from typing import Any

from .data_loader import DataStore, ZONES
from .queries import execute_query

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [resp-gen] %(message)s")
log = logging.getLogger(__name__)

DATA_PATH = os.getenv("DATA_PATH", "/app/data/buildings_rm.csv")
store: DataStore | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global store
    log.info(f"Cargando dataset desde {DATA_PATH}")
    store = DataStore(DATA_PATH)
    log.info("Generador de Respuestas listo")
    yield
    log.info("Shutting down")


app = FastAPI(title="Generador de Respuestas", lifespan=lifespan)


class QueryRequest(BaseModel):
    query_type: str = Field(..., description="Q1|Q2|Q3|Q4|Q5")
    params: dict[str, Any] = Field(default_factory=dict)


class QueryResponse(BaseModel):
    result: dict[str, Any]
    compute_time_ms: float


# Estado para simular fallas temporales
simulate_failure = False


@app.get("/health")
async def health():
    return {"status": "ok", "dataset_loaded": store is not None, "simulate_failure": simulate_failure}


@app.post("/toggle_failure")
async def toggle_failure(body: dict | None = None):
    global simulate_failure
    if body and "enabled" in body:
        simulate_failure = bool(body["enabled"])
    else:
        simulate_failure = not simulate_failure
    log.info(f"Simulate failure toggled to: {simulate_failure}")
    return {"simulate_failure": simulate_failure}


@app.get("/stats")
async def stats():
    if store is None:
        raise HTTPException(503, "Dataset no cargado aun")
    return {
        "zones": {zid: {"name": ZONES[zid]["name"],
                        "n_buildings": len(df)}
                  for zid, df in store.by_zone.items()},
        "total_buildings": sum(len(df) for df in store.by_zone.values()),
    }


@app.post("/query", response_model=QueryResponse)
def query(req: QueryRequest):
    if simulate_failure:
        log.warning("Simulando falla temporal: Servicio no disponible")
        raise HTTPException(503, "Simulated service unavailable")

    if store is None:
        raise HTTPException(503, "Dataset no cargado")
    t0 = time.perf_counter()
    try:
        result = execute_query(store, req.query_type, req.params)
    except (KeyError, ValueError) as e:
        raise HTTPException(400, str(e))
    compute_ms = (time.perf_counter() - t0) * 1000
    return QueryResponse(result=result, compute_time_ms=compute_ms)

