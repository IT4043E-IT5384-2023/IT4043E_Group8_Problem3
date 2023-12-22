from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, when, mean
from pyspark.sql.functions import regexp_replace
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import json

spark = (SparkSession.builder.appName("group08").master("spark://34.142.194.212:7077")
         .config("spark.jars", "/opt/spark/jars/gcs-connector-latest-hadoop2.jar")
         .config("spark.executor.memory", "1G")  #excutor excute only 2G
        .config("spark.driver.memory","4G") 
        .config("spark.debug.maxToStringFields", "1000000") 
        .config("spark.executor.cores","1") #Cluster use only 3 cores to excute as it has 3 server
        .config("spark.python.worker.memory","1G") # each worker use 1G to excute
        .config("spark.driver.maxResultSize","3G") #Maximum size of result is 3G
        .config("spark.kryoserializer.buffer.max","1024M")
         .config("spark.port.maxRetries", "100")
         .getOrCreate())
#config the credential to identify the google cloud hadoop file 
spark.conf.set("google.cloud.auth.service.account.json.keyfile","/opt/bucket_connector/lucky-wall-393304-3fbad5f3943c.json")
spark._jsc.hadoopConfiguration().set('fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem')
spark._jsc.hadoopConfiguration().set('fs.gs.auth.service.account.enable', 'true')

es = Elasticsearch(
    "http://34.143.255.36:9200",  # Replace with your Elasticsearch instance URL
    basic_auth=('elastic', 'elastic2023'),  # Optional: Include if your instance has authentication enabled
)

data = spark.read.json("gs://it4043e-it5384/it4043e/it4043e_group8_problem3/raw/processed_data/part-00001-d28e2a62-8b1b-41a5-8026-f995051da0cf-c000.json")
data_dicts = data.toJSON().map(lambda x: json.loads(x)).collect()

actions = [
    {
        "_index": "group08",  # Replace with your index name
        "_source": record
    }
    for record in data_dicts
]

bulk(es, actions)
spark.stop()