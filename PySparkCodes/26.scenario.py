from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("TwoDataFrames").getOrCreate()

# First DataFrame
data1 = [(1, "A"), (2, "B"), (3, "C"), (4, "D")]
sourcedf = spark.createDataFrame(data1, ["id", "name"])
sourcedf.show()

# Second DataFrame
data2 = [(1, "A"), (2, "B"), (4, "X"), (5, "F")]
targetdf = spark.createDataFrame(data2, ["id1", "name1"])
targetdf.show()



from pyspark.sql.functions import *

joindf=sourcedf.join(targetdf,sourcedf["id"]==targetdf["id1"]     ,"outer")

fildf = joindf.filter((col("name") != col("name1")) | col("name").isNull() | col("name1").isNull())
fildf.show()

filnulldf = fildf.withColumn("id",coalesce(col("id"),col("id1"))).drop("id1")
filnulldf.show()

finaldf = filnulldf.withColumn("comment",expr("case when name is null then 'new in target' when name1 is null then 'new in source' when name != name1 then 'mismatch' end")).drop("name","name1")
finaldf.show()


#
#=========================



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

#
#


# select
#   id,
#   case when name != name1 then 'Mismatch' when name1 is null then 'New in Source' when name is null then 'New in Target' end as comment
# from
#   (
#     select
#       coalesce(id, id1) as id,
#       s.name,
#       t.name1
#     from
#       sourcetab s full
#       outer join targettab t on s.id = t.id1
#     WHERE
#       s.name != t.name1
#       OR s.name IS NULL
#       OR t.name1 IS NULL
#   );