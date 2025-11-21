from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.getOrCreate()

df = spark.createDataFrame(
    [
        (1, 300, "31-Jan-2021"),
        (1, 400, "28-Feb-2021"),
        (1, 200, "31-Mar-2021"),
        (2, 1000, "31-Oct-2021"),
        (2, 900, "31-Dec-2021")
    ],
    ["empid", "commissionamt", "monthlastdate"]
)

df.show()


#df.groupby("empid").agg(max("monthlastdate").alias("maxsale")).show()

maxdatedf = df.groupBy(col("empid").alias("empid")).agg(max("monthlastdate").alias("maxdate"))
maxdatedf.show()


joindf = df.join(maxdatedf, (df["empid"] == maxdatedf["empid"]) & (df["monthlastdate"] == maxdatedf["maxdate"]),
                 "inner").drop("empid", "maxdate")

joindf.show()