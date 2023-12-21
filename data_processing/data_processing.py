from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, when, mean



spark = (SparkSession.builder.appName("group08").master("spark://34.142.194.212:7077")
         .config("spark.jars", "/opt/spark/jars/gcs-connector-latest-hadoop2.jar")
         .config("spark.executor.memory", "2G")  #excutor excute only 2G
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

def data_cleansing(i):
    kols_path=f"gs://it4043e-it5384/it4043e/it4043e_group8_problem3/raw/R_{i}/kols_table.csv"
    tweets_path=f"gs://it4043e-it5384/it4043e/it4043e_group8_problem3/raw/R_{i}/tweets_table.csv"
    
    kols_df = spark.read.csv(kols_path, header=True, inferSchema=True)
    tweets_df = spark.read.csv(tweets_path, header=True, inferSchema=True, quote='"',escape='"',multiLine=True)

    
    non_empty_columns = [c for c in kols_df.columns if kols_df.filter(kols_df[c].isNotNull()).count() > 0]
    kols_df = kols_df.select(*non_empty_columns)
    selected_columns = kols_df.columns[6:13]  
    for column in selected_columns:
        mean_value = kols_df.select(mean(col(column))).collect()[0][0]
        kols_df = kols_df.withColumn(column, when(col(column).isNull(), mean_value).otherwise(col(column)))
        kols_df = kols_df.withColumn(column, col(column).cast(IntegerType()))
        kols_df = kols_df.withColumn("is_verified_num", when(col("is_verified"), 1).otherwise(0))
    combined_df = kols_df.join(tweets_df, kols_df.user_id == tweets_df.author_id)
    combined_df = combined_df.dropDuplicates()
    final_df = combined_df.select("user_id", "tweet_body")
    return final_df
saved_path = f"gs://it4043e-it5384/it4043e/it4043e_group8_problem3/raw/processed_data/test"
df = data_cleansing(18)
df.write.mode("append").json(saved_path)
spark.stop()
