"""
Job de Spark Structured Streaming para la Tarea 3.

Lee eventos de metrics-topic, los agrega en ventanas de tiempo y los escribe
en Elasticsearch. Corre continuamente y esta desacoplado del resto del sistema.
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, BooleanType, IntegerType,
)

# ---------------------------------------------------------------------------
# Configuracion (via variables de entorno)
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
METRICS_TOPIC = os.getenv("METRICS_TOPIC", "metrics-topic")

ES_NODES = os.getenv("ES_NODES", "elasticsearch")
ES_PORT = os.getenv("ES_PORT", "9200")
ES_INDEX = os.getenv("ES_INDEX", "metrics-aggregated")

WINDOW_DURATION = os.getenv("WINDOW_DURATION", "1 minute")
SLIDE_DURATION = os.getenv("SLIDE_DURATION", "10 seconds")
WATERMARK = os.getenv("WATERMARK", "30 seconds")
TRIGGER_INTERVAL = os.getenv("TRIGGER_INTERVAL", "10 seconds")
STARTING_OFFSETS = os.getenv("STARTING_OFFSETS", "latest")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/tmp/spark-checkpoint")

# Segundos de la ventana, para escalar el throughput a "por minuto".
WINDOW_SECONDS = 60.0


# Esquema de los eventos publicados por el Sistema de Metricas en metrics-topic.
EVENT_SCHEMA = StructType([
    StructField("ts", DoubleType(), True),
    StructField("timestamp", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("query_type", StringType(), True),
    StructField("latency_ms", DoubleType(), True),
    StructField("cache_hit", BooleanType(), True),
    StructField("was_retried", BooleanType(), True),
    StructField("is_retry_event", BooleanType(), True),
    StructField("retry_count", IntegerType(), True),
    StructField("status", StringType(), True),
    StructField("key", StringType(), True),
])


def build_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("MetricsStructuredStreaming")
        .config("es.nodes", ES_NODES)
        .config("es.port", ES_PORT)
        .config("es.nodes.wan.only", "true")
        .config("es.index.auto.create", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def write_batch_to_es(batch_df, batch_id: int):
    """Sink: escribe cada micro-batch agregado en Elasticsearch (upsert por id)."""
    if batch_df.rdd.isEmpty():
        return
    (
        batch_df.write
        .format("org.elasticsearch.spark.sql")
        .option("es.nodes", ES_NODES)
        .option("es.port", ES_PORT)
        .option("es.nodes.wan.only", "true")
        .option("es.mapping.id", "window_id")
        .option("es.write.operation", "upsert")
        .mode("append")
        .save(ES_INDEX)
    )
    print(f"[spark] batch {batch_id}: {batch_df.count()} ventanas -> {ES_INDEX}",
          flush=True)


def main():
    spark = build_spark()

    # 1. Leer el stream desde Kafka.
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", METRICS_TOPIC)
        .option("startingOffsets", STARTING_OFFSETS)
        .option("failOnDataLoss", "false")
        .load()
    )

    # 2. Parsear los mensajes JSON y derivar la columna de tiempo de evento.
    events = (
        raw.select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("d"))
        .select("d.*")
        .withColumn("event_time", F.col("ts").cast("timestamp"))
        .where(F.col("event_time").isNotNull())
    )

    # Latencia considerada solo para consultas exitosas (hit/miss/recovery).
    success_latency = F.when(F.col("status") == "success", F.col("latency_ms"))

    # 3. + 4. Ventanas de tiempo deslizantes + agregaciones.
    agg = (
        events
        .withWatermark("event_time", WATERMARK)
        .groupBy(F.window("event_time", WINDOW_DURATION, SLIDE_DURATION))
        .agg(
            F.sum(F.when(F.col("status") == "success", 1).otherwise(0)).alias("count_success"),
            F.sum(F.when(F.col("cache_hit") == True, 1).otherwise(0)).alias("count_hit"),    # noqa: E712
            F.sum(F.when(F.col("event_type") == "miss", 1).otherwise(0)).alias("count_miss"),
            F.sum(F.when(F.col("event_type") == "recovery", 1).otherwise(0)).alias("count_recovery"),
            F.sum(F.when(F.col("event_type") == "retry", 1).otherwise(0)).alias("count_retry"),
            F.sum(F.when(F.col("event_type") == "dlq", 1).otherwise(0)).alias("count_dlq"),
            F.sum(F.when(F.col("event_type") == "error", 1).otherwise(0)).alias("count_error"),
            F.count(F.lit(1)).alias("count_events"),
            F.percentile_approx(success_latency, 0.5, 10000).alias("latency_p50"),
            F.percentile_approx(success_latency, 0.95, 10000).alias("latency_p95"),
            F.percentile_approx(success_latency, 0.99, 10000).alias("latency_p99"),
            F.avg(success_latency).alias("latency_avg"),
        )
    )

    # 5. Derivar metricas finales (tasas + throughput por minuto).
    out = (
        agg
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end", F.col("window.end"))
        .withColumn("@timestamp", F.col("window.end"))
        .withColumn("window_id", F.col("window.start").cast("long").cast("string"))
        .withColumn(
            "throughput_per_min",
            F.round(F.col("count_success") * (60.0 / F.lit(WINDOW_SECONDS)), 2),
        )
        .withColumn(
            "hit_rate",
            F.when(F.col("count_success") > 0,
                   F.round(F.col("count_hit") / F.col("count_success"), 4)).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "retry_rate",
            F.when((F.col("count_success") + F.col("count_dlq")) > 0,
                   F.round(F.col("count_retry") /
                           (F.col("count_success") + F.col("count_dlq")), 4)).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "dlq_rate",
            F.when((F.col("count_success") + F.col("count_dlq")) > 0,
                   F.round(F.col("count_dlq") /
                           (F.col("count_success") + F.col("count_dlq")), 4)).otherwise(F.lit(0.0)),
        )
        .drop("window")
    )

    # 6. Sink continuo hacia Elasticsearch via foreachBatch (output mode update).
    query = (
        out.writeStream
        .outputMode("update")
        .foreachBatch(write_batch_to_es)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )

    print(f"[spark] Streaming iniciado: {METRICS_TOPIC} -> ES index '{ES_INDEX}' "
          f"(ventana={WINDOW_DURATION}, slide={SLIDE_DURATION})", flush=True)
    query.awaitTermination()


if __name__ == "__main__":
    main()
