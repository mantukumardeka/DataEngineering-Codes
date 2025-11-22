from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

data = [
    ("m1", "m1,m2", "m1,m2,m3", "m1,m2,m3,m4")
]

df = spark.createDataFrame(data, ["col1", "col2", "col3", "col4"])
df.show(truncate=False)


from pyspark.sql.functions import *

df1=df.withColumn("col", expr("concat(col1,'-',col2,'-',col3,'-',col4)"  )).drop("col1","col2","col3","col4")

df1.show()

df2=df1.selectExpr(  "explode( split(col,'-')  ) as col"  )

df2.show()