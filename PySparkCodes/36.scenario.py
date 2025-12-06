from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Products").getOrCreate()

data = [
    ("2020-05-30", "Headphone"),
    ("2020-06-01", "Pencil"),
    ("2020-06-02", "Mask"),
    ("2020-05-30", "Basketball"),
    ("2020-06-01", "Book"),
    ("2020-06-02", "Mask"),
    ("2020-05-30", "T-Shirt")
]

df = spark.createDataFrame(data, ["sell_date", "product"])
df.show()


from pyspark.sql.functions import *

df1=df.groupby("sell_date").agg( collect_set("product").alias("Products") ,size(collect_set("product")).alias("counts")  )

df1.show()