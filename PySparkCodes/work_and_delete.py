# from pyspark.sql import SparkSession
#
# # Initialize Spark Session
# spark = SparkSession.builder.appName("CreateDataFrameExample").getOrCreate()
#
# # Define sample data
# data = [
#     ("00000", "06-26-2011", 200, "Exercise", "GymnasticsPro", "cash"),
#     ("00001", "05-26-2011", 300, "Exercise", "Weightlifting", "credit"),
#     ("00002", "06-01-2011", 100, "Exercise", "GymnasticsPro", "cash"),
#     ("00003", "06-05-2011", 100, "Gymnastics", "Rings", "credit"),
#     ("00004", "12-17-2011", 300, "Team Sports", "Field", "paytm"),
#     ("00005", "02-14-2011", 200, "Gymnastics", None, "cash")
# ]
#
# # Define column names
# columns = ["id", "tdate", "amount", "category", "product", "spendby"]
#
# # Create DataFrame
# df = spark.createDataFrame(data, columns)
#
# # Show DataFrame
# df.show()
#
# # df.select("id","tdate").show()
# #
# # df.drop("id","tdate").show()
# #
# # df.show()
#
# # df.filter("category='Exercise'").show()
# # df.filter("category='Exercise' and spendby ='cash'").show()
#
#
# # select will not work-
# procdf = df.selectExpr(
#     "id",
#     "amount",
#     "upper(category) as category",
#     "product",
#     "spendby"
# )
#
# procdf.show()
#
#
#
# procdf = df.selectExpr(
#     "id",
#     "split(tdate, '-')[2] as year",
#     "amount + 100 as amount",
#     "upper(category) as category",
#     "concat(product, '~zeyo') as product",
#     "spendby"
# )
#
# procdf.show()
#
#
# from pyspark.sql.functions import expr
#
# procdf1 = (
#     df.withColumn("category", expr("upper(category)"))
#       .withColumn("amount", expr("amount + 100"))
#       .withColumn("product", expr("concat(product, '~zeyo')"))
#       .withColumn("tdate", expr("split(tdate, '-')[2]"))
#       .withColumn("status", expr("case when spendby = 'cash' then 0 else 1 end"))
#       .withColumnRenamed("tdate", "year")
# )
#
# procdf1.show()
#



from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_list

# Initialize Spark
spark = SparkSession.builder.appName("GroupCollectExample").getOrCreate()

# Create DataFrame
data = [
    (1, "aaa", "apple"),
    (2, "bbb", "samsung"),
    (1, "aaa", "mi"),
    (3, "ccc", "redmi")
]

columns = ["id", "name", "dep"]
df = spark.createDataFrame(data, columns)

# Group and aggregate
result_df = (
    df.groupBy("id", "name")
      .agg(collect_list("dep").alias("dep"))
)

# Show result as DataFrame
result_df.show(truncate=False)
