from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("PriceData").getOrCreate()

data = [
    (1, "26-May", 100),
    (1, "27-May", 200),
    (1, "28-May", 300),
    (2, "29-May", 400),
    (3, "30-May", 500),
    (3, "31-May", 600)
]

columns = ["pid", "date", "price"]

df = spark.createDataFrame(data, columns)

df.show()


df.createTempView("tab22")

spark.sql("select pid,date, price, sum(price) over(partition by pid order by price rows between unbounded preceding and current row) as newprice  from tab22  ").show()


print("USING DF")

from pyspark.sql.functions import *
from pyspark.sql.window import Window

wins=Window.partitionBy("pid").orderBy("price")

df1=df.withColumn("newPrice", sum("price").over(wins))

df1.show()