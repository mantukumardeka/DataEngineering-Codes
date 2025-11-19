from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

data = [
    (1, "a", 10000),
    (2, "b", 5000),
    (3, "c", 15000),
    (4, "d", 25000),
    (5, "e", 50000),
    (6, "f", 7000)
]

columns = ["empid", "name", "salary"]

df = spark.createDataFrame(data, columns)
df.show()
