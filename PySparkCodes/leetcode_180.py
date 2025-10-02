# Example 1:
#
# Input:
# Logs table:
# +----+-----+
# | id | num |
# +----+-----+
# | 1  | 1   |
# | 2  | 1   |
# | 3  | 1   |
# | 4  | 2   |
# | 5  | 1   |
# | 6  | 2   |
# | 7  | 2   |
# +----+-----+
# Output:
# +-----------------+
# | ConsecutiveNums |
# +-----------------+
# | 1               |
# +-----------------+
# Explanation: 1 is the only number that appears consecutively for at least three times.


from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window



spark = SparkSession.builder.appName("LeetCode_180").getOrCreate()

# Data as list of tuples
logs_data = [
    (1, 1),
    (2, 1),
    (3, 1),
    (4, 2),
    (5, 1),
    (6, 2),
    (7, 2)
]

columns = ["id", "num"]

logs_df = spark.createDataFrame(data=logs_data, schema=columns)
logs_df.show()

print("Using DSL")

from pyspark.sql import functions as F

windowSpec=Window.orderBy(F.col("id").desc())

diff_df=logs_df.withColumn("Prevnum", lag("num",1).over(windowSpec)).withColumn("NextNum", lead("num",1).over(windowSpec))

diff_df.show()

print("Now filtering it- ")
#
# cons_num_df=diff_df.filter((F.col("num")== F.col("NextNum")) & (F.col("num") == F.col("Prevnum")))
#
# cons_num_df.show()
#
# print("Using SQL")

# cons_num_df = diff_df.filter((F.col("num") == F.col("Prevnum")) & (F.col("num") == F.col("NextNum")))

# cons_num_df.show()


# Filter rows where num equals both Prevnum and NextNum
cons_num_df = diff_df.filter(
    (F.col("num") == F.col("Prevnum")) & (F.col("num") == F.col("NextNum"))
)

# Count such rows and return as single column 'connum'
result_df = cons_num_df.agg(F.count("*").alias("connum"))

result_df.show()


