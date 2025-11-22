from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("MM").getOrCreate()

source_rdd = spark.sparkContext.parallelize([
    (1, "A"),
    (2, "B"),
    (3, "C"),
    (4, "D")
],1)

target_rdd = spark.sparkContext.parallelize([
    (1, "A"),
    (2, "B"),
    (4, "X"),
    (5, "F")
],2)

# Convert RDDs to DataFrames using toDF()
df1 = source_rdd.toDF(["id", "name"])
df2 = target_rdd.toDF(["id", "name1"])

# Show the DataFrames
df1.show()
df2.show()


joindf  = df1.join(df2, ["id"] , "full")
joindf.show()

from pyspark.sql.functions import *

casedf = joindf.withColumn("comment",expr("""
                                           case
                                           when name=name1 then 'match'
                                           else 'mismatch'
                                           end
                                            """))

casedf.show()