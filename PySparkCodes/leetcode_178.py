
# -- Write a SQL query to rank scores. If there is a tie, both should have the same ranking.
# -- After a tie, the next ranking number should be the next consecutive integer (no holes).
# Scores table:
# +----+-------+
# | id | score |
# +----+-------+
# | 1  | 3.50  |
# | 2  | 3.65  |
# | 3  | 4.00  |
# | 4  | 3.85  |
# | 5  | 4.00  |
# | 6  | 3.65  |
# +----+-------+


from pyspark.sql import *

from pyspark.sql.functions import *

from PySparkCodes.leetcode_176 import windowspace

spark=SparkSession.builder.appName("DENSE_RANK").getOrCreate()


# Sample data as list of tuples
data = [
    (1, 3.50),
    (2, 3.65),
    (3, 4.00),
    (4, 3.85),
    (5, 4.00),
    (6, 3.65)
]

columns = ["id", "score"]

scrore_df=spark.createDataFrame(data, columns)

scrore_df.show()

print("using Window Fucntions")

# from pyspark.sql.functions import window,dense_rank
#
# from pyspark.sql import SparkSession
from pyspark.sql import functions
from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank
#
from pyspark.sql.functions import *

# # Define WindowSpec (order scores in descending order)

windowspace=Window.orderBy(functions.col("score").desc())

ranksdf=scrore_df.withColumn("rank",dense_rank().over(windowspace))

ranksdf.show()

