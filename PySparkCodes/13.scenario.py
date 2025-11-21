from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("emp_df").getOrCreate()

data = [
    (1, "Jhon", "Development"),
    (2, "Tim", "Development"),
    (3, "David", "Testing"),
    (4, "Sam", "Testing"),
    (5, "Green", "Testing"),
    (6, "Miller", "Production"),
    (7, "Brevis", "Production"),
    (8, "Warner", "Production"),
    (9, "Salt", "Production")
]

df = spark.createDataFrame(data, ["emp_id", "emp_name", "dept"])
df.show()

from pyspark.sql.functions import *

df1=df.groupBy("dept").agg(count("emp_id").alias("Count"))

df1.show()

## USing MYSQL

df.createTempView("tab13")

spark.sql("select dept, count('emp_id') as counts from tab13 group by dept" ).show()