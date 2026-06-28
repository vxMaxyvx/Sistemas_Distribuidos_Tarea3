#!/bin/bash
set -e

# Pequena espera para asegurar que metricas ya creo `metrics-topic` y que
# Elasticsearch este completamente operativo antes de iniciar el stream.
SLEEP_BEFORE_START="${SLEEP_BEFORE_START:-25}"
echo "[spark] Esperando ${SLEEP_BEFORE_START}s a Kafka/Elasticsearch..."
sleep "${SLEEP_BEFORE_START}"

exec /opt/spark/bin/spark-submit \
    --master "local[2]" \
    --packages "${SPARK_KAFKA_PKG},${ES_SPARK_PKG}" \
    --conf spark.jars.ivy=/tmp/.ivy2 \
    --conf spark.sql.shuffle.partitions=4 \
    --conf spark.sql.session.timeZone=UTC \
    /app/job.py
