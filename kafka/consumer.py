from datetime import datetime
import os
import sys
PROJECT_ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../")
sys.path.append(PROJECT_ROOT)

from dotenv import load_dotenv
load_dotenv(f"{PROJECT_ROOT}/configs/infra.env")
KAFKA_BOOTSTRAP_SERVER = os.getenv('KAFKA_BOOTSTRAP_SERVER')
KAFKA_TOPIC = os.getenv('CRL_KAFKA_TOPIC')
GCS_BUCKET = os.getenv('BUCKET_PATH')
SPARK_MASTER = os.getenv('SPARK_MASTER')

print(KAFKA_BOOTSTRAP_SERVER)

import json

from kafka import KafkaConsumer
from utils.custlog import custlogger
logger = custlogger("Consumer 0")

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import when, from_json

class Consumer():
    def __init__(self):
        # kafka producer
        self.consumer = KafkaConsumer(
            group_id="consumer0",
            auto_offset_reset='earliest',
            bootstrap_servers=[KAFKA_BOOTSTRAP_SERVER],
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            consumer_timeout_ms=2000
        )

        self.consumer.subscribe(KAFKA_TOPIC)

        _spark = (SparkSession.builder.appName("group08").master(SPARK_MASTER)
            .config("spark.jars",
                    "spark/jars/gcs-connector-hadoop3-latest.jar")
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.elasticsearch:elasticsearch-spark-30_2.12:8.11.2") \
            .config("spark.executor.memory", "1G")  # execute only 2G
            .config("spark.driver.memory","4G") 
            .config("spark.debug.maxToStringFields", "1000000") 
            .config("spark.executor.cores","1") # Cluster use only 3 cores to excute as it has 3 server
            .config("spark.python.worker.memory","1G") # each worker use 1G to excute
            .config("spark.driver.maxResultSize","3G") # Maximum size of result is 3G
            .config("spark.kryoserializer.buffer.max","1024M")
            .config("spark.port.maxRetries", "100")
            .getOrCreate())

        _spark.conf.set("google.cloud.auth.service.account.json.keyfile","/opt/bucket_connector/lucky-wall-393304-3fbad5f3943c.json")
        _spark._jsc.hadoopConfiguration().set('fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem')
        _spark._jsc.hadoopConfiguration().set('fs.gs.auth.service.account.enable', 'true')

        self._spark = _spark

    def get_message(self):
        # Read messages from Kafka                
        df = self._spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER) \
            .option("kafka.group.id", "consumer0") \
            .option("subscribe", KAFKA_TOPIC) \
            .option("startingOffsets", "earliest") \
            .load()

        # Convert value column from Kafka to string
        df = df.selectExpr("CAST(value AS STRING)")        
        return df

    def save_data(self, df, id):
        # Define the schema to extract specific fields
        schema = StructType() \
            .add("id", StringType()) \
            .add("username", StringType()) \
            .add("name", StringType()) \
            .add("created_at", StringType()) \
            .add("is_verified", StringType()) \
            .add("media_count", IntegerType()) \
            .add("statuses_count", IntegerType()) \
            .add("favourites_count", IntegerType()) \
            .add("followers_count", IntegerType()) \
            .add("normal_followers_count", IntegerType()) \
            .add("friends_count", IntegerType()) \
            .add("possibly_sensitive", IntegerType()) \
            .add("fast_followers_count", IntegerType()) \
            .add("profile_url", StringType()) \
            .add("protected", StringType()) \
            .add("description", StringType())

        df = df \
            .select(from_json(df.value, schema).alias("data")) \
            .select("data.*")

        # write to GCS as CSV
        df  .coalesce(1) \
            .write \
            .mode("append") \
            .option("header", "true") \
            .csv(f"{PROJECT_ROOT}/{datetime.now()}.csv")

        logger.info(f"Saved data as CSV to GCS")

    def consume(self):
        # sink to GCS as RT streaming is not supported with WebAPI crawler
        try:
            df = self.get_message()

            stream = df \
                .writeStream \
                .trigger(processingTime='30 seconds') \
                .foreachBatch(self.save_data) \
                .outputMode("append") \
                .start()
            
            stream.awaitTermination(3600)

        except Exception as e:
            logger.error(e)


if __name__ == "__main__":
    consumer = Consumer()
    consumer.consume()