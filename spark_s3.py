from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv
load_dotenv()
spark = SparkSession.builder \
     .appName("myApp") \
     .config("spark.hadoop.fs.s3a.access.key", os.getenv('s3_access_key')) \
     .config("spark.hadoop.fs.s3a.secret.key", os.getenv('s3_secret_key')) \
     .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
     .config("spark.jars","./jar_files/hadoop-aws-3.3.2.jar , ./jar_files/aws-java-sdk-bundle-1.12.448.jar ,./jar_files/aws-java-sdk-s3-1.12.448.jar") \
     .getOrCreate()

s3_file_url=os.getenv('s3_file_url')
print(s3_file_url)
df=spark.read.csv(os.getenv('s3_file_url'),inferSchema=True)
df.show()
