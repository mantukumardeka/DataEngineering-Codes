from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

data = [
    (1, "Mark Ray", "AB"),
    (2, "Peter Smith", "CD"),
    (1, "Mark Ray", "EF"),
    (2, "Peter Smith", "GH"),
    (2, "Peter Smith", "CD"),
    (3, "Kate", "IJ")
]

columns = ["custid", "custname", "address"]

df = spark.createDataFrame(data, columns)

df.show()

from pyspark.sql.functions import *


df.groupBy("custid","custname").agg(collect_set("address")).show()

