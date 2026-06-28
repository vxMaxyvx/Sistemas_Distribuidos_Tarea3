#!/usr/bin/env bash
#
# Tarea 3 - Configuracion automatica de Elasticsearch + Kibana.
#
# 1. Crea un index template para `metrics-aggregated*` con los tipos correctos
#    (campos de fecha como `date`, metricas como numericos).
# 2. Crea el Data View en Kibana (time field = window_end).
# 3. Importa el dashboard y las visualizaciones (kibana/dashboard.ndjson).
#
# Ejecutar DESPUES de `docker compose up -d` y de que Kibana este disponible:
#     bash kibana/setup.sh
#
set -euo pipefail

ES_URL="${ES_URL:-http://localhost:9200}"
KIBANA_URL="${KIBANA_URL:-http://localhost:5601}"
INDEX="${ES_INDEX:-metrics-aggregated}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "==> Esperando a Elasticsearch en ${ES_URL} ..."
until curl -s "${ES_URL}/_cluster/health" | grep -qE '"status":"(yellow|green)"'; do
  sleep 3
done
echo "    Elasticsearch OK"

echo "==> Creando index template para '${INDEX}*' ..."
curl -s -X PUT "${ES_URL}/_index_template/${INDEX}-template" \
  -H 'Content-Type: application/json' -d "{
  \"index_patterns\": [\"${INDEX}*\"],
  \"template\": {
    \"settings\": { \"number_of_shards\": 1, \"number_of_replicas\": 0 },
    \"mappings\": {
      \"properties\": {
        \"@timestamp\":        { \"type\": \"date\" },
        \"window_start\":      { \"type\": \"date\" },
        \"window_end\":        { \"type\": \"date\" },
        \"window_id\":         { \"type\": \"keyword\" },
        \"throughput_per_min\":{ \"type\": \"double\" },
        \"latency_p50\":       { \"type\": \"double\" },
        \"latency_p95\":       { \"type\": \"double\" },
        \"latency_p99\":       { \"type\": \"double\" },
        \"latency_avg\":       { \"type\": \"double\" },
        \"hit_rate\":          { \"type\": \"double\" },
        \"retry_rate\":        { \"type\": \"double\" },
        \"dlq_rate\":          { \"type\": \"double\" },
        \"count_success\":     { \"type\": \"long\" },
        \"count_hit\":         { \"type\": \"long\" },
        \"count_miss\":        { \"type\": \"long\" },
        \"count_recovery\":    { \"type\": \"long\" },
        \"count_retry\":       { \"type\": \"long\" },
        \"count_dlq\":         { \"type\": \"long\" },
        \"count_error\":       { \"type\": \"long\" },
        \"count_events\":      { \"type\": \"long\" }
      }
    }
  }
}" >/dev/null
echo "    Index template creado"

echo "==> Esperando a Kibana en ${KIBANA_URL} ..."
until curl -s "${KIBANA_URL}/api/status" | grep -q '"level":"available"'; do
  sleep 5
done
echo "    Kibana OK"

echo "==> Creando Data View '${INDEX}*' (time field: window_end) ..."
curl -s -X POST "${KIBANA_URL}/api/data_views/data_view" \
  -H 'kbn-xsrf: true' -H 'Content-Type: application/json' -d "{
  \"data_view\": {
    \"title\": \"${INDEX}*\",
    \"name\": \"${INDEX}\",
    \"timeFieldName\": \"window_end\"
  }
}" >/dev/null || echo "    (Data View ya existia o se omitio)"
echo "    Data View listo"

echo "==> Importando dashboard y visualizaciones ..."
curl -s -X POST "${KIBANA_URL}/api/saved_objects/_import?overwrite=true" \
  -H 'kbn-xsrf: true' \
  --form file=@"${SCRIPT_DIR}/dashboard.ndjson" >/dev/null
echo "    Dashboard importado"

echo ""
echo "Listo. Abre Kibana -> Dashboards -> 'Tarea 3 - Monitoreo en Tiempo Real'"
echo "URL directa: ${KIBANA_URL}/app/dashboards#/view/t3-dashboard"
