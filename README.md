# teju
Engineer
 version: "3"
services:
  kafka:
    image: wurstmeister/kafka
    ports:
      - "9093:9093"
    environment:
      - KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:9093
      - KAFKA_LISTENER_SECURITY_PROTOCOL=PLAINTEXT

  spark:
    image: bitnami/spark
    ports:
      - "7077:7077"
    environment:
      - SPARK_MASTER=spark://spark:7077
      - KAFKA_BROKER=kafka:9093

  cassandra:
    image: cassandra:latest
    ports:
      - "9042:9042"

  airflow:
    image: puckel/docker-airflow
    environment:
      - AIRFLOW__CORE__EXECUTOR=LocalExecutor
    ports:
      - "8080:8080"
    volumes:
      - ./dags:/opt/airflow/dags


from kafka import KafkaProducer
import json

producer = KafkaProducer(bootstrap_servers='localhost:9093', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Send a message (data)
message = {"event": "user_login", "user_id": 12345}
producer.send('user-events', message)
producer.flush()

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Kafka-Spark-Consumer") \
    .getOrCreate()

# Read data from Kafka
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9093") \
    .option("subscribe", "user-events") \
    .load()

# Extract value (message) and deserialize it
message_df = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", schema_of_json('{"event":"string","user_id":"integer"}')).alias("data")) \
    .select("data.event", "data.user_id")

# Perform some transformations or aggregations (e.g., count events)
aggregated_df = message_df.groupBy("event").count()

# Write the result to Cassandra
aggregated_df.writeStream \
    .outputMode("complete") \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "events_keyspace") \
    .option("table", "event_counts") \
    .start() \
    .awaitTermination()

CREATE KEYSPACE IF NOT EXISTS events_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS events_keyspace.event_counts (
    event TEXT PRIMARY KEY,
    count INT
);



