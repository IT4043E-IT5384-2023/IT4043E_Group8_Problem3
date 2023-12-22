from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, when, mean
from pyspark.sql.functions import regexp_replace
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import json

spark = (SparkSession.builder.appName("group08").master("spark://<SPARK_IP>:7077")
         .config("spark.jars", "path_to_gcs-connector-latest-hadoop2.jar")
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
spark.conf.set("google.cloud.auth.service.account.json.keyfile","path_to_CREDENTIAL_FILE")
spark._jsc.hadoopConfiguration().set('fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem')
spark._jsc.hadoopConfiguration().set('fs.gs.auth.service.account.enable', 'true')

es = Elasticsearch(
    "http://<ELASTIC_IP>:9200",  # Replace with your Elasticsearch instance URL
    basic_auth=('elastic', 'elastic2023'),  # Optional: Include if your instance has authentication enabled
)

data = spark.read.json("path_to_json_file")
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
