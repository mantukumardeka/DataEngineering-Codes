from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("tab26").getOrCreate()

data = [
    (1, 60000, 2018),
    (1, 70000, 2019),
    (1, 80000, 2020),
    (2, 60000, 2018),
    (2, 65000, 2019),
    (2, 65000, 2020),
    (3, 60000, 2018),
    (3, 65000, 2019)
]

df = spark.createDataFrame(data, ["empid", "salary", "year"])
df.show()

print("USING MYSQL")
df.createTempView("tab26")

spark.sql("select * from tab26").show()

spark.sql("""  select empid,salary,year, lag(salary,1) over(partition by empid order by year) as diff from tab26
 
 """).show()

spark.sql(""" select empid,salary,year, coalesce( (salary-diff),0 ) as increment  from (select empid,salary,year, lag(salary,1) over(partition by empid order by year) as diff from tab26)

 """).show()

print("USING DSL")


df.show()

from pyspark.sql.functions import *
from pyspark.sql.window import Window

wn=Window.partitionBy("empid").orderBy("year")

df1=df.withColumn("diff", lag("salary",1).over(wn)   )

# df2=df1.select(col(" empid"),col("salary"), col("year "))
#
# df2.show()

from pyspark.sql.functions import col, lag, coalesce, lit
from pyspark.sql.window import Window

wn = Window.partitionBy("empid").orderBy("year")

df1 = df.withColumn("diff", lag("salary", 1).over(wn))

df2 = df1.withColumn(
    "increment",
    col("salary") - coalesce(col("diff"), lit(0))
).drop(col("diff"))

df2.show()

# wn = Window.partitionBy("empid").orderBy("year")
#
# lagdf = df.withColumn("diff",lag("salary",1).over(wn))
# lagdf.show()
#
# finaldf = lagdf.withColumn("incresalary",expr("salary - diff")).drop("diff").na.fill(0).orderBy("empid","year")
#
# finaldf.show()
