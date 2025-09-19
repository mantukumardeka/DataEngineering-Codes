from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys
import os

conf = SparkConf().setAppName("pyspark").setMaster("local[*]").set("spark.driver.host","localhost").set("spark.default.parallelism", "1")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

spark = SparkSession.builder.getOrCreate()


df = spark.read.load("s3://zeyos44/src/")

df.show()

aggdf = df.groupBy("username").agg(count("ip").alias("ipcount"))

aggdf.show()

aggdf.write.mode("overwrite").save("s3://zeyos44/dest/ipcount")








import subprocess

subprocess.run(["sudo", "yum", "install", "-y", "ca-certificates"], check=True)
subprocess.run(["sudo", "update-ca-trust", "force-enable"], check=True)


