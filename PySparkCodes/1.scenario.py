from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

spark = SparkSession.builder.appName("WorkerData").getOrCreate()

# Define schema
schema = StructType([
    StructField("workerid", StringType(), True),
    StructField("firstname", StringType(), True),
    StructField("lastname", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("joiningdate", StringType(), True),
    StructField("department", StringType(), True)
])

# Data
data = [
    ("001", "Monika", "Arora", 100000, "2014-02-20 09:00:00", None),
    ("002", "Niharika", "Verma", 300000, "2014-06-11 09:00:00", "Admin"),
    ("003", "Vishal", "Singhal", 300000, "2014-02-20 09:00:00", "HR"),
    ("004", "Amitabh", "Singh", 500000, "2014-02-20 09:00:00", "Admin"),
    ("005", "Vivek", "Bhati", 500000, "2014-06-11 09:00:00", "Admin")
]

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Show the DataFrame
df.show(truncate=False)

## UsinG SQL:

df.createTempView("Emp")

spark.sql("select a.workerid,a.firstname,a.lastname,a.salary,a.joiningdate,a.department from emp a join emp b on a.workerid!=b.workerid and a.salary=b.salary").show()

# using DSL

from pyspark.sql.functions import col

df.alias("a").join(df.alias("b"),(col("a.workerid")!=col("b.workerid")) & (col("a.salary")==col("b.salary")),"inner"     ).select( col("a.workerid"),col("a.firstname"),col("a.lastnamegit"),col("a.salary") ).show()
