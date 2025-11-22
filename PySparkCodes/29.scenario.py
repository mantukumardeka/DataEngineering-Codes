from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("tab29").getOrCreate()

# DataFrame 1
df1 = spark.createDataFrame(
    [(1,), (2,), (3,)],
    ["col"]
)

# DataFrame 2
df2 = spark.createDataFrame(
    [(1,), (2,), (3,), (4,), (5,)],
    ["col1"]
)

df1.show()
df2.show()


from pyspark.sql.functions import *


maxdf = df1.agg(max("col").alias("max"))
maxdf.show()

maxsalary = maxdf.select(col("max")).first()[0]

joindf = df1.join(df2, df1["col"] == df2["col1"], "outer").drop("col")
joindf.show()

finaldf = joindf.filter(col("col1") != maxsalary).withColumnRenamed("col1", "col").orderBy("col")
finaldf.show()