
#Creating a RDD using PySpark

from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext

spark = SparkSession .builder .appName("Code") .getOrCreate()

data=["Apple","Banana","Orange","Cherry",]

rdd=spark.sparkContext.parallelize(data)
print(rdd.collect())


#Reading RDD from local-

print("Reading RDD from lacal")

rdd1 = spark.sparkContext.textFile("/Users/mantukumardeka/Desktop/data/scdata.txt")
print(rdd1.collect())


