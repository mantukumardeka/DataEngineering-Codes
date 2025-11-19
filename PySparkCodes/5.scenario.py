from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()



data = [
    (1, "abc", 31, "abc@gmail.com"),
    (2, "def", 23, "defyahoo.com"),
    (3, "xyz", 26, "xyz@gmail.com"),
    (4, "qwe", 34, "qwegmail.com"),
    (5, "iop", 24, "iop@gmail.com")
]

columns = ["id", "name", "age", "email"]

df = spark.createDataFrame(data, columns)

df.show()

data1 = [
    (11, "jkl", 22, "abc@gmail.com", 1000),
    (12, "vbn", 33, "vbn@yahoo.com", 3000),
    (13, "wer", 27, "wer", 2000),
    (14, "zxc", 30, "zxc.com", 2000),
    (15, "lkj", 29, "lkj@outlook.com", 2000)
]

columns = ["id", "name", "age", "email", "salary"]

df1 = spark.createDataFrame(data1, columns)

df1.show()

from pyspark.sql.functions import *


df2=df.withColumn("salary", lit(1000))

df2.show()

print("UNION RESULT")

uniondf=df1.union(df2).orderBy("id").filter(col("email").contains("@"))

uniondf.show()



print("Using SQL")

