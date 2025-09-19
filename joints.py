# # from pyspark.sql import SparkSession
#TS
# # from pyspark.sql.types import StructType, StructField, IntegerType, StringType
# #
# #
# # spark = SparkSession.builder.appName("JoinExample").getOrCreate()
# #
# # # Customers DataFrame
# # customers = [
# #     (1, "Ravi"),
# #     (2, "Gautham"),
# #     (3, "Mary"),
# #     (4, "Thomas")
# # ]
# # customers_df = spark.createDataFrame(customers, ["customer_id", "name"])
# #
# # # Orders DataFrame
# # orders = [
# #     (101, 1),
# #     (102, 3)
# # ]
# # orders_df = spark.createDataFrame(orders, ["order_id", "customer_id"])
# #
# #
# # customers_df.show(5)
# # orders_df.show(5)
# #
# #
# # print("LEFT JOINT")
# #
# #
# # customers_df.join(orders_df, "customer_id","left").show()
# #
# #
# # print("LEFT ANTI JOIN")
# #
# #
# # customers_df.join(orders_df, "customer_id","left_anti").show()
# #
# #
# # print("LEFT SEMI JOINT")
# #
# # df1=customers_df.join(orders_df, "customer_id","left_semi")
# #
# # df1.show()
# #
# #
# # df1.write.format("csv").mode("overwrite").save("/Users/mantukumardeka/Desktop/output/csv_out")
#
#
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, count, sum as _sum, round
#
# # Start Spark
# spark = SparkSession.builder.appName("PercentageExample").getOrCreate()
#
# # Sample Data
# data = [
#     (123, "y"),
#     (123, "y"),
#     (123, "n"),
#     (123, "n"),
#     (123, None),
#     (123, "y")
# ]
#
# columns = ["ID", "flag"]
# df = spark.createDataFrame(data, columns)
#
# # Count by ID and flag
# count_df = df.groupBy("ID", "flag").agg(count("*").alias("cnt"))
#
# # Get total per ID
# total_df = count_df.groupBy("ID").agg(_sum("cnt").alias("total"))
#
# # Join to calculate percentage
# result_df = count_df.join(total_df, "ID") \
#     .withColumn("percentage", round((col("cnt") / col("total")) * 100, 2))
#
# result_df.show()

# Reverse string without using built-in functions
def reverse_string(s):
    result = ""
    for char in s:
        result = char + result
    return result

print(reverse_string("Cognizant"))  # Output: tnizgnoC
