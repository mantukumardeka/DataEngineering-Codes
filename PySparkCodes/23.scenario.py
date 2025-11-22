from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CustomerProducts").getOrCreate()

data = [
    (1, 5),
    (2, 6),
    (3, 5),
    (3, 6),
    (1, 6)
]

df = spark.createDataFrame(data, ["customer_id", "product_key"])

df.show()

df.select("product_Key").distinct().show()