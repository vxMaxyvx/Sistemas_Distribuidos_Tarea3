"""
Script para generar el dataset de edificaciones para la Region Metropolitana
de Santiago, basado en las distribuciones del dataset Google Open Buildings.

Los datos se generan de forma sintetica siguiendo las distribuciones
estadisticas del dataset real (areas log-normal, confianza beta).
Cada zona tiene una cantidad de edificaciones proporcional a su
densidad urbana real.
"""
import csv
import os
import numpy as np

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")
OUTPUT_FILE = os.path.join(DATA_DIR, "buildings_rm.csv")


def main():
    os.makedirs(DATA_DIR, exist_ok=True)

    if os.path.exists(OUTPUT_FILE):
        print(f"Dataset ya existe: {OUTPUT_FILE}")
        print("Elimina el archivo si quieres regenerarlo.")
        return

    generate_data()


def generate_data():
    """
    Genera datos sinteticos basados en la distribucion real esperada
    del dataset Google Open Buildings para Santiago.
    """
    np.random.seed(42)

    zones = {
        "Z1": {"lat_min": -33.445, "lat_max": -33.420, "lon_min": -70.640, "lon_max": -70.600, "n": 8500},
        "Z2": {"lat_min": -33.420, "lat_max": -33.390, "lon_min": -70.600, "lon_max": -70.550, "n": 9200},
        "Z3": {"lat_min": -33.530, "lat_max": -33.490, "lon_min": -70.790, "lon_max": -70.740, "n": 7800},
        "Z4": {"lat_min": -33.460, "lat_max": -33.430, "lon_min": -70.670, "lon_max": -70.630, "n": 11000},
        "Z5": {"lat_min": -33.470, "lat_max": -33.430, "lon_min": -70.810, "lon_max": -70.760, "n": 6500},
    }

    all_buildings = []
    for zone_id, z in zones.items():
        lats = np.random.uniform(z["lat_min"], z["lat_max"], z["n"])
        lons = np.random.uniform(z["lon_min"], z["lon_max"], z["n"])
        # Areas: distribucion log-normal, tipico de edificaciones urbanas
        areas = np.random.lognormal(mean=4.5, sigma=0.8, size=z["n"])
        areas = np.clip(areas, 10, 5000)
        # Confidence: beta distribution sesgada a valores altos (como el dataset real)
        confidences = np.random.beta(a=5, b=1.5, size=z["n"])
        confidences = np.clip(confidences, 0.65, 1.0)

        for i in range(z["n"]):
            all_buildings.append({
                "latitude": round(lats[i], 6),
                "longitude": round(lons[i], 6),
                "area_in_meters": round(areas[i], 2),
                "confidence": round(confidences[i], 4),
            })

    np.random.shuffle(all_buildings)

    with open(OUTPUT_FILE, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["latitude", "longitude", "area_in_meters", "confidence"])
        writer.writeheader()
        writer.writerows(all_buildings)

    print(f"Dataset generado: {len(all_buildings)} edificaciones")
    print(f"Archivo: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
