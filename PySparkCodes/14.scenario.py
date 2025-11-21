from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("student_df").getOrCreate()

data = [
    (203040, "rajesh", 10, 20, 30, 40, 50)
]

df = spark.createDataFrame(
    data,
    ["rollno", "name", "telugu", "english", "maths", "science", "social"]
)

df.show()

from pyspark.sql.functions import *

# df1=df.withColumn("total", expr(sum("telugu"+ "english"+"maths"+ "science"+ "social")))
#
# df1.show()


# Method 1: Using expr()
df1 = df.withColumn("total", expr("telugu + english + maths + science + social"))
df1.show()

df.createTempView("tab14")

spark.sql("select * , (telugu+english+maths+science+social) as total from tab14").show()