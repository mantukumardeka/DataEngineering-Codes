from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("emp_df").getOrCreate()

data = [
    (1, "Jhon", "Testing", 5000),
    (2, "Tim", "Development", 6000),
    (3, "Jhon", "Development", 5000),
    (4, "Sky", "Prodcution", 8000)
]

df = spark.createDataFrame(
    data,
    ["id", "name", "dept", "salary"]
)

df.show()

## using SQL

# df.createTempView("tab15")
#
# pyspark.sql(""" DELETE t
# FROM tab15 t
# JOIN (
#     SELECT id,
#            ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn
#     FROM tab15
# ) x
# ON t.id = x.id
# WHERE x.rn = 2;
# """ ).show()

# df.show()
#
# df1=df.dropDuplicates("dept","name")

# df1.show()

print("cleane df")
#
# == DROP duplicate will work if you have real duplicate data, dupliate row:
# df_clean = df.dropDuplicates(["name", "dept"])
# df_clean.show()

df.show()

from pyspark.sql.window import Window

from pyspark.sql.functions import *

wins=Window.partitionBy("dept").orderBy(col("id").desc())

dropdf=df.withColumn("rows", row_number().over(wins)).filter("rows==1").drop("rows")


df.show()

print("Using ROW NUMBER")
dropdf.show()


print(" Deleting using name as duplicate")
finaldf = df.dropDuplicates(["name"]).orderBy("id")
finaldf.show()


print("Deleating using dept as duplicate")
depdf=df.dropDuplicates(["dept"]).orderBy("id")

depdf.show()