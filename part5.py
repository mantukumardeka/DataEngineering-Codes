

print("Part 5 InterView Questions:")
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark=SparkSession.builder.appName("Part5 INTERVIEW QUESTIONS").getOrCreate()

#
# data = [
#     (1, "m", "1,2", 20),
#     (2, "f", "1,2,3", 20),
#     (3, "T", "1", 20)
# ]
#
# # Create DataFrame
# columns = ["emp_id", "gender", "category", "age"]
# df = spark.createDataFrame(data, columns)
# df.show()
#
# df_normalized=df.withColumn("Category",explode(split(df.category,',')))
# df.withColumn("gender", when(df.gender=="m","Male").when(df.gender=="f", "female").otherwise("other")).show()


print("Next Questions")


#Below will through error:
# data=[1,2,2,3,4]
# df=spark.createDataFrame(data,["ID"])
# df.show()

data = [(1,), (2,), (2,), (3,), (4,)]   # notice each row is a tuple
df = spark.createDataFrame(data, ["ID"])
df.show()

from pyspark.sql.functions import rank
from pyspark.sql.window import Window


windowSpec = Window.orderBy(df["ID"])

df1=df.withColumn("rank", rank().over(windowSpec)) \
              .withColumn("dense_rank", dense_rank().over(windowSpec))

df1.show()