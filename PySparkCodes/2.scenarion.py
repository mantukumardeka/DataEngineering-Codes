from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

data = [
    (1, "1-Jan", "Ordered"),
    (1, "2-Jan", "Dispatched"),
    (1, "3-Jan", "Dispatched"),
    (1, "4-Jan", "Shipped"),
    (1, "5-Jan", "Shipped"),
    (1, "6-Jan", "Delivered"),
    (2, "1-Jan", "Ordered"),
    (2, "2-Jan", "Dispatched"),
    (2, "3-Jan", "Shipped")
]

df = spark.createDataFrame(data, ["orderid", "statusdate", "status"])

df.show()


from pyspark.sql.functions import *

df.filter("status=='Dispatched'").show()

df.filter(col("status")=='Dispatched').show()

## Using SQL
df.createTempView("tab")

spark.sql("select * from tab where status='Dispatched'").show()
