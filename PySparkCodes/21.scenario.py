from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("scenario21").getOrCreate()

data = [
    ("SEA", "SF", 300),
    ("CHI", "SEA", 2000),
    ("SF", "SEA", 300),
    ("SEA", "CHI", 2000),
    ("SEA", "LND", 500),
    ("LND", "SEA", 500),
    ("LND", "CHI", 1000),
    ("CHI", "NDL", 180)
]

columns = ["from", "to", "dist"]

df = spark.createDataFrame(data, columns)

# Correct renaming
df = df.withColumnRenamed("from", "from_city") \
       .withColumnRenamed("to", "to_city")

df.createOrReplaceTempView("tab21")


df.show()

# Round-trip distances
spark.sql("""
SELECT 
    a.from_city,
    a.to_city,
    a.dist + b.dist AS distance
FROM tab21 a
JOIN tab21 b
    ON a.from_city = b.to_city
   AND a.to_city = b.from_city
WHERE a.from_city < a.to_city
""").show()

from pyspark.sql.functions import *
# Self join to find round-trip distances
df.alias("a").join(
    df.alias("b"),
    (col("a.from_city") == col("b.to_city")) & (col("a.to_city") == col("b.from_city"))
).where(col("a.from_city") < col("a.to_city")) \
.select(
    col("a.from_city"),
    col("a.to_city"),
    (col("a.dist") + col("b.dist")).alias("round_trip_dist")
).show()