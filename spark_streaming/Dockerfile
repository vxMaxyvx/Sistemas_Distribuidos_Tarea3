FROM apache/spark:3.5.1

# Conectores necesarios:
#   - spark-sql-kafka-0-10  : fuente de streaming desde Kafka
#   - elasticsearch-spark-30: sink hacia Elasticsearch
# Versiones alineadas con Spark 3.5.1 (Scala 2.12) y Elasticsearch 8.13.x.
ENV SPARK_KAFKA_PKG="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1" \
    ES_SPARK_PKG="org.elasticsearch:elasticsearch-spark-30_2.12:8.13.4" \
    SPARK_HOME="/opt/spark" \
    IVY_DIR="/tmp/.ivy2" \
    HOME="/tmp"

USER root
WORKDIR /app

# Pre-descargar (cachear) los jars de los conectores en tiempo de build,
# ejecutando un submit no-op que fuerza la resolucion de dependencias via Ivy.
# Asi el contenedor arranca sin depender de la red en cada ejecucion.
RUN mkdir -p /tmp/.ivy2 && echo "print('deps cached')" > /tmp/_resolve.py && \
    /opt/spark/bin/spark-submit \
        --master "local[1]" \
        --packages "${SPARK_KAFKA_PKG},${ES_SPARK_PKG}" \
        --conf spark.jars.ivy=/tmp/.ivy2 \
        /tmp/_resolve.py && rm -f /tmp/_resolve.py

COPY job.py /app/job.py
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh && chmod -R 777 /tmp/.ivy2 /tmp

USER 185

ENTRYPOINT ["/app/entrypoint.sh"]
