from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.getOrCreate()

df = spark.createDataFrame(
    [
        ("a", [1, 1, 1, 3]),
        ("b", [1, 2, 3, 4]),
        ("c", [1, 1, 1, 1, 4]),
        ("d", [3])
    ],
    ["name", "rank"]
)

df.show(truncate=False)

# (write spark code, list of name of participants who has rank=1 most number of times)

df.printSchema()
df1=df.withColumn("rank", explode(col("rank")))

df1.show()

df1.printSchema()


df2=df1.filter(col("rank")==1)

df2.show()

df3=df2.groupby("name").agg(count("*").alias("count"))

df3.show()


finaldf = df3.select(col("name")).first()[0]
print(finaldf)
