from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys
import os

conf = SparkConf().setAppName("pyspark").setMaster("local[*]").set("spark.driver.host","localhost").set("spark.default.parallelism", "1")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

spark = SparkSession.builder.getOrCreate()

ipdf = spark.read.load("s3://zeyos44/dest/ipcount")

scoresdf = spark.read.load("s3://zeyos44/dest/scores")

joindf = ipdf.join(scoresdf,["username"],"inner")

joindf.write.mode("overwrite").save("s3://zeyos44/dest/master")


#--packages  net.snowflake:spark-snowflake_2.12:3.1.3