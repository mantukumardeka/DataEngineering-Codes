from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

data = [
    (1111, "2021-01-15", 10),
    (1111, "2021-01-16", 15),
    (1111, "2021-01-17", 30),
    (1112, "2021-01-15", 10),
    (1112, "2021-01-15", 20),
    (1112, "2021-01-15", 30)
]

df = spark.createDataFrame(data, ["sensorid", "timestamp", "values"])
df.show()


## SQL

df.createTempView("sensor")


spark.sql(""" SELECT sensorid,
       timestamp,
       ( newvalues - values ) AS values
FROM  (SELECT *,
              Lead(values, 1, 0)
                OVER(
                  partition BY sensorid
                  ORDER BY values) AS newvalues
       FROM   sensor)
WHERE  newvalues != 0 """ ).show()


# DSL

from pyspark.sql.functions import *
from pyspark.sql.window import Window


df1=Window.partitionBy("sensorid").orderBy("values")
#
df.withColumn( "newvalue", lead("values",1).over(df1)).filter(col("newvalue").isNotNull())\
    .withColumn("updat_evalue", expr("newvalue- values")).drop("newvalue").show()





# d1 = Window.partitionBy("sensorid").orderBy("values")
#
# finaldf = df.withColumn("nextvalues", lead("values", 1).over(d1))\
#     .filter(col("nextvalues").isNotNull()) \
#     .withColumn("values", expr("nextvalues-values")) \
#     .drop("nextvalues") \
#     .orderBy(col("sensorid"))
#
# finaldf.show()