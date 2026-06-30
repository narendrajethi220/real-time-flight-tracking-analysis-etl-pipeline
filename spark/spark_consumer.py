from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    round,
    to_timestamp,
    from_unixtime,
    current_timestamp,
    avg,
    window,
    approx_count_distinct
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    LongType
)

import mysql.connector
from dotenv import load_dotenv
import os

# LOADING ENVIRONMENT VARIABLES

dotenv_path = os.path.join(
    os.path.dirname(os.path.dirname(__file__)),
    ".env"
)

load_dotenv(dotenv_path)

KAFKA_SERVER = os.getenv("KAFKA_SERVER")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")

S3_CLEAN_PATH = os.getenv("S3_CLEAN_PATH")
S3_CLEAN_CHECKPOINT = os.getenv("S3_CLEAN_CHECKPOINT")

MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_PORT = os.getenv("MYSQL_PORT", "3306")


# CREATE SPARK SESSION

spark = (
    SparkSession.builder
    .appName("flight-tracking")
    .config("spark.sql.streaming.metricsEnabled", "false")
    .config("spark.sql.streaming.ui.enabled", "false")
    .config("spark.metrics.conf.*.sink*console.class", "")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("Spark Started Successfully")
print("Kafka Server :", KAFKA_SERVER)
print("Kafka Topic  :", KAFKA_TOPIC)


# KAFKA SOURCE

kafka_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_SERVER)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

print("Kafka Source Loaded")


# SCHEMA

flight_schema = StructType([
    StructField("event_time", LongType(), True),
    StructField("icao24", StringType(), True),
    StructField("callsign", StringType(), True),
    StructField("origin_country", StringType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("altitude", DoubleType(), True),
    StructField("velocity", DoubleType(), True)
])


# PARSE JSON

parsed_df = (
    kafka_stream
    .selectExpr("CAST(value AS STRING) AS json_value")
    .select(
        from_json(
            col("json_value"),
            flight_schema
        ).alias("data")
    )
    .select("data.*")
)


# CLEANING + TRANSFORMATION + WATERMARKING

clean_df = (
    parsed_df
    .filter(col("longitude").isNotNull())
    .filter(col("latitude").isNotNull())
    .filter(col("icao24").isNotNull())
    .withColumn(
        "speed_kmh",
        round(col("velocity") * 3.6, 2)
    )
    .withColumn(
        "event_timestamp",
        to_timestamp(
            from_unixtime(col("event_time"))
        )
    )
    .withColumn(
        "ingestion_time",
        current_timestamp()
    )
    .withWatermark("event_timestamp", "10 minutes")
)


# WRITE CLEAN DATA TO S3
s3_query = (
    clean_df.writeStream
    .format("parquet")
    .option("path", S3_CLEAN_PATH)
    .option(
        "checkpointLocation",
        f"{S3_CLEAN_CHECKPOINT}_s3"
    )
    .trigger(processingTime="60 seconds")
    .start()
)
print("S3 Streaming Query Started")


# WINDOW AGGREGATION

windowed_stats = (
    clean_df
    .groupBy(
        window(col("event_timestamp"), "5 minutes"),
        col("origin_country")
    )
    .agg(
        approx_count_distinct("icao24").alias("flight_count"),
        round(avg("altitude"), 2).alias("avg_altitude"),
        round(avg("speed_kmh"), 2).alias("avg_speed")
    )
)

# WRITE AGGREGATIONS TO MYSQL

def process_mysql_batch(batch_df, batch_id):
    try:
        if batch_df.isEmpty():
            print(f"Batch {batch_id} is empty")
            return

        print(f"Processing MySQL Batch {batch_id}")

        rows = batch_df.collect()
        
        if not rows:
            return

        conn = mysql.connector.connect(
            host=MYSQL_HOST,
            port=int(MYSQL_PORT) if MYSQL_PORT else 3306,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE
        )

        cursor = conn.cursor()

        upsert_sql = """
        INSERT INTO country_flight_stats
        (
            window_start,
            window_end,
            origin_country,
            flight_count,
            avg_altitude,
            avg_speed
        )
        VALUES (%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
            flight_count = VALUES(flight_count),
            avg_altitude = VALUES(avg_altitude),
            avg_speed = VALUES(avg_speed)
        """

        for row in rows:
            cursor.execute(
                upsert_sql,
                (
                    row["window"]["start"],
                    row["window"]["end"],
                    row["origin_country"],
                    int(row["flight_count"]),
                    float(row["avg_altitude"]) if row["avg_altitude"] is not None else None,
                    float(row["avg_speed"]) if row["avg_speed"] is not None else None
                )
            )

        conn.commit()
        cursor.close()
        conn.close()

        print(f"Batch {batch_id} written to MySQL")
    except Exception as e:
        print(f"Batch {batch_id} failed: {e}")

mysql_query = (
    windowed_stats.writeStream
    .foreachBatch(process_mysql_batch)
    .outputMode("update")
    .option(
        "checkpointLocation",
        f"{S3_CLEAN_CHECKPOINT}_mysql"
    )
    .trigger(processingTime="60 seconds")
    .start()
)
print("MySQL Streaming Query Started")

spark.streams.awaitAnyTermination()