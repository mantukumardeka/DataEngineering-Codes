from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys
import os

conf = SparkConf().setAppName("pyspark").setMaster("local[*]").set("spark.driver.host","localhost").set("spark.default.parallelism", "1")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

spark = SparkSession.builder.getOrCreate()

import subprocess

subprocess.run(["sudo", "yum", "install", "-y", "ca-certificates"], check=True)
subprocess.run(["sudo", "update-ca-trust", "force-enable"], check=True)


secret = subprocess.getoutput("aws   secretsmanager get-secret-value --secret-id  snowpassword --query SecretString --output text")



import   json
password = json.loads(secret)["snowpassword"]
print(password)




df = (
    spark.read.format("snowflake")
    .option("sfURL","http://xogowaf-tg85536.snowflakecomputing.com")
    .option("sfAccount","xogowaf")
    .option("sfUser","zeyodanda")
    .option("sfPassword", password )
    .option("sfDatabase","zeyodb")
    .option("sfSchema","zeyoschema")
    .option("sfRole","ACCOUNTADMIN")
    .option("sfWarehouse","COMPUTE_WH")
    .option("dbtable","scores")
    .load()
)

df.show()

aggdf = df.groupBy("username").agg(collect_list("score").alias("scores"))

aggdf.write.mode("overwrite").save("s3://zeyos44/dest/scores")
