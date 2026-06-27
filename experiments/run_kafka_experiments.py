"""
run_kafka_experiments.py
Bateria de experimentos automatizados para la Tarea 2.
Controla Docker Compose (escala y variables de entorno), inyecta fallas temporales
y genera los snapshots de resultados en results/.
"""
import os
import json
import time
import subprocess
import urllib.request
import urllib.error
from pathlib import Path

# URLs de los servicios en el host
TRAFFIC = "http://localhost:5003"
CACHE = "http://localhost:5000"
METRICS = "http://localhost:5002"
RESPONSE_GEN = "http://localhost:5001"

RESULTS_DIR = Path(__file__).parent.parent / "results"
RESULTS_DIR.mkdir(parents=True, exist_ok=True)


def post(url, body=None, timeout=30):
    data = json.dumps(body or {}).encode()
    req = urllib.request.Request(
        url, data=data,
        headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read())
    except Exception as e:
        print(f"Error en POST a {url}: {e}")
        return None


def get(url, timeout=10):
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            return json.loads(r.read())
    except Exception as e:
        print(f"Error en GET a {url}: {e}")
        return None


PROJECT_ROOT = Path(__file__).parent.parent


def run_cmd(cmd):
    """Ejecuta un comando del sistema desde el directorio raiz del proyecto."""
    res = subprocess.run(cmd, shell=True, capture_output=True, text=True, cwd=str(PROJECT_ROOT))
    if res.returncode != 0:
        print(f"Error ejecutando: {cmd}\nStdout: {res.stdout}\nStderr: {res.stderr}")
    return res.stdout.strip()


def restart_services(use_kafka=False, scale_consumers=1):
    """Reinicia y escala los servicios segun el modo del experimento."""
    print(f"\n[docker] Reiniciando servicios con USE_KAFKA={use_kafka} y scale={scale_consumers}...", flush=True)
    
    # Detener contenedores
    run_cmd("docker compose down")
    time.sleep(2)
    
    # Configurar variable de entorno para la sesion
    env = os.environ.copy()
    env["USE_KAFKA"] = "true" if use_kafka else "false"
    
    # Iniciar servicios con escala
    cmd = f"docker compose up -d --build --scale cache_api={scale_consumers}"
    print(f"  Ejecutando: {cmd}")
    subprocess.run(cmd, shell=True, env=env, cwd=str(PROJECT_ROOT))
    
    # Esperar a que esten listos
    wait_for_services()


def wait_for_services(retries=40, interval=2.0):
    """Espera a que los servicios esten saludables."""
    for svc, url in [("respuestas", RESPONSE_GEN), ("metricas", METRICS), ("trafico", TRAFFIC)]:
        print(f"  Esperando a {svc}...", end="", flush=True)
        for _ in range(retries):
            try:
                res = get(f"{url}/health", timeout=2)
                if res and res.get("status") == "ok":
                    print(" OK", flush=True)
                    break
            except Exception:
                pass
            time.sleep(interval)
        else:
            print(" FAILED", flush=True)
            raise RuntimeError(f"El servicio {svc} no respondio.")


def run_experiment(label, dist, duration, rate, use_kafka, scale, simulated_failure=False, extra_config=None):
    """Ejecuta un experimento individual y toma snapshot."""
    print(f"\n>>> INICIANDO EXPERIMENTO: {label} (USE_KAFKA={use_kafka}, scale={scale})", flush=True)
    
    # Inicializar servicios
    restart_services(use_kafka=use_kafka, scale_consumers=scale)
    
    # Esperar a que los consumidores Kafka se conecten y reciban particiones
    if use_kafka:
        print("  Esperando asignacion de particiones Kafka...", end="", flush=True)
        time.sleep(15)
        print(" OK", flush=True)
    
    # Limpiar cache y resetear metricas
    post(f"{METRICS}/reset")
    run_cmd("docker compose exec redis redis-cli FLUSHDB")
    
    cfg = {
        "distribution": dist,
        "rate_qps": float(rate),
        "duration_sec": float(duration),
        "zipf_s": 1.2,
        "concurrency": max(16, int(rate) // 3),
        "seed": 42,
        "label": label,
    }
    
    # Lanzar trafico
    print(f"  Lanzando trafico: {rate} QPS por {duration}s...", flush=True)
    post(f"{TRAFFIC}/run", cfg)
    
    start_time = time.time()
    
    # Monitorear ejecucion y opcionalmente inyectar falla
    failure_triggered = False
    failure_restored = False
    
    deadline = start_time + duration + 30
    
    backlog_history = []
    
    while time.time() < deadline:
        # Monitorear estado
        status = get(f"{TRAFFIC}/status")
        if not status or not status.get("running"):
            # Si el trafico termino y el backlog en Kafka es 0, terminamos!
            if use_kafka:
                summary_data = get(f"{METRICS}/summary")
                lag = summary_data.get("backlog_size", 0) if summary_data else 0
                if lag == 0:
                    break
            else:
                break
                
        # Guardar historial de backlog si estamos en Kafka
        if use_kafka:
            summary_data = get(f"{METRICS}/summary")
            lag = summary_data.get("backlog_size", 0) if summary_data else 0
            backlog_history.append({"time_offset": round(time.time() - start_time, 1), "backlog": lag})
            
        # Simular falla temporal
        elapsed = time.time() - start_time
        if simulated_failure:
            # Inyectar falla a los 30 segundos (despues de warm-up del cache)
            if elapsed >= 30.0 and not failure_triggered:
                # 1. Activar falla PRIMERO (antes de flush)
                post(f"{RESPONSE_GEN}/toggle_failure", {"enabled": True})
                # 2. Flush cache para forzar misses (response_gen ya esta caido)
                run_cmd("docker compose exec redis redis-cli FLUSHDB")
                print("\n  [FALLA] Falla activada + Cache flushed (HTTP 503)", flush=True)
                failure_triggered = True
            
            # Restaurar servicio a los 60 segundos (30 segundos de caida)
            if elapsed >= 60.0 and not failure_restored:
                print("\n  [FALLA] Restaurando Generador de Respuestas! Comienza recuperacion...", flush=True)
                post(f"{RESPONSE_GEN}/toggle_failure", {"enabled": False})
                failure_restored = True
                
        time.sleep(1.0)
        
    time.sleep(2.0)
    
    # Guardar snapshot de metricas
    snap_body = {
        "label": label,
        "extra": {
            "use_kafka": use_kafka,
            "scale": scale,
            "simulated_failure": simulated_failure,
            "backlog_history": backlog_history,
            **(extra_config or {})
        }
    }
    
    snap = post(f"{METRICS}/snapshot", snap_body)
    if snap:
        # Guardar localmente en results/
        out_path = RESULTS_DIR / f"snap_{label}.json"
        with open(out_path, "w") as f:
            json.dump(snap, f, indent=2)
        print(f"  Snapshot guardado exitosamente en: {out_path}", flush=True)
    else:
        print("  ERROR: No se pudo capturar el snapshot.", flush=True)


def run_spike_experiment():
    """
    Escenario 6: Spike REAL de trafico (3 fases).
    Fase 1: 80 QPS x 35s  (operacion normal, warm-up)
    Fase 2: 320 QPS x 15s (spike: 4x la tasa normal)
    Fase 3: 80 QPS x 60s  (vuelta a normal, drenado del backlog)
    """
    label = "6_kafka_traffic_spike"
    print(f"\n{'='*60}\n[EXPERIMENTO] {label}\n{'='*60}\n", flush=True)

    restart_services(use_kafka=True, scale_consumers=1)

    print("  Esperando asignacion de particiones Kafka...", end="", flush=True)
    time.sleep(15)
    print(" OK", flush=True)

    post(f"{METRICS}/reset")
    run_cmd("docker compose exec redis redis-cli FLUSHDB")

    start_time = time.time()
    backlog_history = []

    phases = [
        {"rate": 80,  "duration": 35, "tag": "normal"},
        {"rate": 320, "duration": 15, "tag": "SPIKE"},
        {"rate": 80,  "duration": 60, "tag": "drain"},
    ]
    phase_idx = 0
    spike_start_time = None
    spike_end_time = None

    def _start_phase(idx):
        ph = phases[idx]
        cfg = {
            "distribution": "zipf",
            "rate_qps": float(ph["rate"]),
            "duration_sec": float(ph["duration"]),
            "zipf_s": 1.2,
            "concurrency": max(20, ph["rate"] // 3),
            "seed": 42,
            "label": label,
        }
        post(f"{TRAFFIC}/run", cfg)
        print(f"  [SPIKE] Fase {idx+1} ({ph['tag']}): {ph['rate']} QPS x {ph['duration']}s", flush=True)

    _start_phase(0)
    deadline = start_time + 240

    while time.time() < deadline:
        elapsed = time.time() - start_time
        status = get(f"{TRAFFIC}/status")
        is_running = status.get("running", False) if status else False

        if not is_running and phase_idx < len(phases) - 1:
            phase_idx += 1
            if phase_idx == 1:
                spike_start_time = round(elapsed, 1)
            elif phase_idx == 2:
                spike_end_time = round(elapsed, 1)
            _start_phase(phase_idx)
        elif not is_running and phase_idx == len(phases) - 1:
            summary_data = get(f"{METRICS}/summary")
            lag = summary_data.get("backlog_size", 0) if summary_data else 0
            if lag == 0:
                break

        summary_data = get(f"{METRICS}/summary")
        lag = summary_data.get("backlog_size", 0) if summary_data else 0
        backlog_history.append({"time_offset": round(elapsed, 1), "backlog": lag})
        time.sleep(1.0)

    time.sleep(2.0)

    snap_body = {
        "label": label,
        "extra": {
            "use_kafka": True,
            "scale": 1,
            "simulated_failure": False,
            "backlog_history": backlog_history,
            "spike_start_time": spike_start_time,
            "spike_end_time": spike_end_time,
            "description": "Spike real: 80 QPS -> 320 QPS (15s) -> 80 QPS",
        }
    }
    snap = post(f"{METRICS}/snapshot", snap_body)
    if snap:
        out_path = RESULTS_DIR / f"snap_{label}.json"
        with open(out_path, "w") as f:
            json.dump(snap, f, indent=2)
        print(f"  Snapshot guardado exitosamente en: {out_path}", flush=True)
    else:
        print("  ERROR: No se pudo capturar el snapshot.", flush=True)


def run_all_scenarios():
    print("="*70)
    print("SISTEMAS DISTRIBUIDOS - BATERIA DE EXPERIMENTOS TAREA 2")
    print("="*70)
    
    t_start = time.time()
    
    # ─────────────────────────────────────────────────────────────────────
    # Escenario 1: Sistema Base (Sincrono, sin Kafka)
    # Referencia para comparar contra la arquitectura asincrona.
    # ─────────────────────────────────────────────────────────────────────
    run_experiment(
        label="1_sync_base",
        dist="zipf",
        duration=120,
        rate=100,
        use_kafka=False,
        scale=1
    )
    
    # ─────────────────────────────────────────────────────────────────────
    # Escenario 2: Kafka + 1 Consumidor
    # Procesamiento asincrono basico con un solo consumer.
    # ─────────────────────────────────────────────────────────────────────
    run_experiment(
        label="2_kafka_1_consumer",
        dist="zipf",
        duration=120,
        rate=100,
        use_kafka=True,
        scale=1
    )
    
    # ─────────────────────────────────────────────────────────────────────
    # Escenario 3a: Kafka + 3 Consumidores
    # Escalamiento horizontal: evaluar impacto de multiples consumers.
    # ─────────────────────────────────────────────────────────────────────
    run_experiment(
        label="3a_kafka_3_consumers",
        dist="zipf",
        duration=120,
        rate=100,
        use_kafka=True,
        scale=3
    )
    
    # ─────────────────────────────────────────────────────────────────────
    # Escenario 3b: Kafka + 5 Consumidores
    # Mas consumidores para mostrar tendencia de escalamiento.
    # ─────────────────────────────────────────────────────────────────────
    run_experiment(
        label="3b_kafka_5_consumers",
        dist="zipf",
        duration=120,
        rate=100,
        use_kafka=True,
        scale=5
    )
    
    # ─────────────────────────────────────────────────────────────────────
    # Escenario 4: Falla Temporal con Kafka (Caida de 30s)
    # Demuestra reintentos, DLQ y recuperacion via colas Kafka.
    # ─────────────────────────────────────────────────────────────────────
    run_experiment(
        label="4_kafka_transient_failure",
        dist="zipf",
        duration=120,
        rate=100,
        use_kafka=True,
        scale=1,
        simulated_failure=True,
        extra_config={"description": "Caida de 30s del Response Gen con reintentos Kafka (1 consumer)"}
    )
    
    # ─────────────────────────────────────────────────────────────────────
    # Escenario 5: Falla Temporal Sincrona (sin Kafka)
    # Comparacion: las consultas se pierden inmediatamente (HTTP 502).
    # ─────────────────────────────────────────────────────────────────────
    run_experiment(
        label="5_sync_transient_failure",
        dist="zipf",
        duration=120,
        rate=100,
        use_kafka=False,
        scale=1,
        simulated_failure=True,
        extra_config={"description": "Caida de 30s del Response Gen en arquitectura sincrona sin colas"}
    )
    
    # ─────────────────────────────────────────────────────────────────────
    # Escenario 6: Spike REAL de Trafico (3 fases)
    # Fase 1 normal (80 QPS x 35s) -> Spike (320 QPS x 15s) -> Drain (80 QPS x 60s).
    # ─────────────────────────────────────────────────────────────────────
    run_spike_experiment()
    
    # ─────────────────────────────────────────────────────────────────────
    # Escenario 7: Recuperacion ante Fallos con Escalamiento
    # Falla prolongada de 15s con 3 consumidores para evaluar recovery
    # time y capacidad de vaciado del backlog post-falla.
    # Comparar con Escenario 5 (sincrono) para medir perdida vs recovery.
    # ─────────────────────────────────────────────────────────────────────
    run_experiment(
        label="7_kafka_recovery_scaled",
        dist="zipf",
        duration=120,
        rate=100,
        use_kafka=True,
        scale=3,
        simulated_failure=True,
        extra_config={"description": "Falla de 30s con 3 consumers para evaluar recovery time y vaciado de backlog"}
    )
    
    # ─────────────────────────────────────────────────────────────────────
    # Escenario 8: Kafka con distribucion Uniforme
    # Comparar patron de trafico uniforme vs Zipf sobre colas Kafka.
    # ─────────────────────────────────────────────────────────────────────
    run_experiment(
        label="8_kafka_uniform",
        dist="uniform",
        duration=120,
        rate=100,
        use_kafka=True,
        scale=1,
        extra_config={"description": "Distribucion uniforme con Kafka para comparar contra Zipf"}
    )
    
    print("\n" + "="*70)
    print(f"BATERIA COMPLETA TERMINADA EN {(time.time() - t_start)/60:.1f} MINUTOS")
    print(f"Resultados en results/ -> graficar con build_kafka_figures.py")
    print("="*70)


if __name__ == "__main__":
    run_all_scenarios()

