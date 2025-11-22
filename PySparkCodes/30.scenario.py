from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("tab30").getOrCreate()

# DataFrame 1
df_emp = spark.createDataFrame(
    [
        (1, "A", "A", 1000000),
        (2, "B", "A", 2500000),
        (3, "C", "G", 500000),
        (4, "D", "G", 800000),
        (5, "E", "W", 9000000),
        (6, "F", "W", 2000000),
    ],
    ["emp_id", "name", "dept_id", "salary"]
)

# DataFrame 2
df_dept = spark.createDataFrame(
    [
        ("A", "AZURE"),
        ("G", "GCP"),
        ("W", "AWS")
    ],
    ["dept_id1", "dept_name"]
)

df_emp.show()
df_dept.show()

from pyspark.sql.functions import *

from pyspark.sql.window import Window

joindf=df_emp.join(df_dept, df_emp["dept_id"]==df_dept["dept_id1"],"inner" )

wn=Window.partitionBy(col("dept_name")).orderBy(col("salary").desc())

finandf=joindf.withColumn("rnk", dense_rank().over(wn)  )


finandf.filter(col("rnk")==2).select(col("emp_id"),col("name"),col("dept_name"),col("salary")).show()