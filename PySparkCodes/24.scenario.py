from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("UserPageVisits").getOrCreate()

data = [
    (1, "home"),
    (1, "products"),
    (1, "checkout"),
    (1, "confirmation"),
    (2, "home"),
    (2, "products"),
    (2, "cart"),
    (2, "checkout"),
    (2, "confirmation"),
    (2, "home"),
    (2, "products")
]

df = spark.createDataFrame(data, ["userid", "page"])

df.show()

from pyspark.sql.functions import *

df.groupby("userid").agg(

collect_list("page")
).show(truncate=False)

print("Using SQL")

df.createTempView("tab24")

spark.sql("select * from tab24").show()

#spark.sql("""select userid ,collect_list("page") as pages group by userid from tab24""").show()

# Correct PySpark SQL
spark.sql("""
    SELECT 
        userid, 
        collect_list(page) AS pages 
    FROM tab24 
    GROUP BY userid
""").show(truncate=False)
