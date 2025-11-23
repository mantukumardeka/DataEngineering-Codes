from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("create_df").getOrCreate()

data = [
    (1, "Alice", 25, "F"),
    (2, "Bob", 40, "M"),
    (3, "Raj", 46, "M"),
    (4, "Sekar", 66, "M"),
    (5, "Jhon", 47, "M"),
    (6, "Timoty", 28, "M"),
    (7, "Brad", 90, "M"),
    (8, "Rita", 34, "F")
]

columns = ["customer_id", "name", "age", "gender"]

df = spark.createDataFrame(data, columns)
df.show()

from pyspark.sql.functions import *


groupdf=df.withColumn("age_group", expr("case when age between 19 and 35 then '19-35' when age between 36 and 50 then '36-50' else '51+' end"))

groupdf.show()

aggdf=groupdf.groupby("age_group").agg( count("*").alias("cnt")).orderBy("age_group")

aggdf.show()

