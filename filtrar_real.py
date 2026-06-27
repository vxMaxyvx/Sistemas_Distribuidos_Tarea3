import pandas as pd
import os

ARCHIVO_ORIGINAL = "data/967_buildings.csv.gz"
CARPETA_DESTINO = "data"
ARCHIVO_FINAL = f"{CARPETA_DESTINO}/buildings_rm.csv"

ZONAS = {
    "Z1": {"lat_min": -33.445, "lat_max": -33.420, "lon_min": -70.640, "lon_max": -70.600},
    "Z2": {"lat_min": -33.420, "lat_max": -33.390, "lon_min": -70.600, "lon_max": -70.550},
    "Z3": {"lat_min": -33.530, "lat_max": -33.490, "lon_min": -70.790, "lon_max": -70.740},
    "Z4": {"lat_min": -33.460, "lat_max": -33.430, "lon_min": -70.670, "lon_max": -70.630},
    "Z5": {"lat_min": -33.470, "lat_max": -33.430, "lon_min": -70.810, "lon_max": -70.760},
}

print("Iniciando lectura por pedacitos (chunks) para no explotar la RAM...")
os.makedirs(CARPETA_DESTINO, exist_ok=True)

# Borramos el archivo final si ya existía a medias por el error anterior
if os.path.exists(ARCHIVO_FINAL):
    os.remove(ARCHIVO_FINAL)

header_escrito = False
total_edificios = 0
pedacito_num = 1

# Leemos el archivo de a 1 millón de filas a la vez
for chunk in pd.read_csv(ARCHIVO_ORIGINAL, compression='gzip', chunksize=1000000):
    print(f"Procesando pedacito #{pedacito_num}...")
    data_filtrada = []
    
    for zona, limites in ZONAS.items():
        filtro = (
            (chunk['latitude'] >= limites['lat_min']) & (chunk['latitude'] <= limites['lat_max']) &
            (chunk['longitude'] >= limites['lon_min']) & (chunk['longitude'] <= limites['lon_max'])
        )
        data_filtrada.append(chunk[filtro])
    
    resultado_chunk = pd.concat(data_filtrada)
    total_edificios += len(resultado_chunk)
    
    # Si encontramos edificios de Santiago en este pedacito, los guardamos
    if not resultado_chunk.empty:
        resultado_chunk.to_csv(ARCHIVO_FINAL, mode='a', index=False, header=not header_escrito)
        header_escrito = True
        
    pedacito_num += 1

print("-" * 30)
print(f"¡ÉXITO REAL! Se filtraron {total_edificios} edificios en total.")
print(f"Archivo guardado en: {ARCHIVO_FINAL}")