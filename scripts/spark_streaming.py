import findspark
findspark.init("C:/spark/spark-3.5.0-bin-hadoop3")
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, FloatType, IntegerType, StringType
import logging
import streamlit as st

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')

logger = logging.getLogger("spark_structured_streaming")


def create_spark_session():
    try:
        spark = (SparkSession.builder \
                 .appName("SparkStructredStreaming") \
                 .config("spark.jars.packages",
                         "com.datastax.spark:spark-cassandra-connector_2.12:3.0.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0") \
                 .master("local[*]") \
                 .config("spark.cassandra.connection.host", "localhost") \
                 .config("spark.cassandra.connection.port", "9042") \
                 .config("spark.cassandra.auth.username", "cassandra") \
                 .config("spark.cassandra.auth.password", "cassandra") \
                 .getOrCreate())
        spark.sparkContext.setLogLevel("ERROR")
        logging.info("spark session successfully established")
        return spark

    except Exception as e:
        logging.error(f"could not create the spark session {e}")


def read_kafka_stream(spark):
    df = (spark.readStream \
          .format("kafka") \
          .option("kafka.bootstrap.servers", "localhost:9092") \
          .option("subscribe", "sensors_topic") \
          .option("delimiter", ",")
          .option("startingOffsets", "earliest") \
          .option("failOnDataLoss", "false")
          .load())
    schema = StructType([
        StructField("ts_min_bignt", IntegerType(), False),
        StructField("co2", FloatType(), False),
        StructField("humidity", FloatType(), False),
        StructField("light", FloatType(), False),
        StructField("pir", FloatType(), False),
        StructField("temperature", FloatType(), False),
        StructField("room", StringType(), False),
        StructField("event_ts_min", StringType(), False)
    ])
    df = df.selectExpr("CAST (value AS STRING)") \
        .select(from_json(col("value"), schema) \
                .alias('data')).select("data.*")

    return df


def stream_to_cassandra(df):
    """
    Starts the streaming to table spark_streaming.random_names in cassandra
    """
    logging.info("Streaming is being started...")
    my_query = (df.writeStream
                .format("org.apache.spark.sql.cassandra")
                .outputMode("append")
                .option("checkpointLocation", "./cassandra/checkpoint")
                .options(table="sensors_data", keyspace="sensors")
                .start())

    my_query.awaitTermination()
    return df



def main():
    spark = create_spark_session()
    df = read_kafka_stream(spark)
    df = stream_to_cassandra(df)



if __name__=="__main__":
    main()

spark.stop()