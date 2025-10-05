#
# -- Question 182: Duplicate Emails
# -- Write a SQL query to find all duplicate emails in the Person table.

# Example 1:

# -- Input:
# -- Person table:
# -- +----+---------+
# -- | id | email   |
# -- +----+---------+
# -- | 1  | a@b.com |
# -- | 2  | c@d.com |
# -- | 3  | a@b.com |
# -- +----+---------+
# -- Output:
# -- +---------+
# -- | Email   |
# -- +---------+
# -- | a@b.com |
# -- +---------+


from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from PySparkCodes.leetcode_180 import windowSpec

spark=SparkSession.builder.appName("Leetcode_182").getOrCreate()

# Data as list of tuples
email_data = [
    (1, "a@b.com"),
    (2, "c@d.com"),
    (3, "a@b.com")
]

columns = ["id", "email"]

email_df=spark.createDataFrame(email_data, columns)
email_df.show()

print("Using DSL-")


from pyspark.sql import Window
from pyspark.sql import functions as F


windowSpec=Window.partitionBy(F.col("email"))


dup_df=(email_df.withColumn("emailcount",   F.count("email").over(windowSpec)) \
        .filter(F.col("emailcount")>1)) \
    .select("email") \
    .distinct()

dup_df.show()




print("Anotherway-Best way")

duplicate_emails = (
    email_df.groupBy("email")
             .agg(F.count("email").alias("count"))
             .filter(F.col("count") > 1)
             .select("email")
)

duplicate_emails.show()



print("Using Spark SQL")

email_df.createOrReplaceTempView("tab")

spark.sql("select email from tab group by email having count(*) >1").show()




