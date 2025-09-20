# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, regexp_replace, sha2
#
# # Create Spark session
# spark = SparkSession.builder.appName("DataMaskingExample").getOrCreate()
#
# # Sample data
# data = [
#     ("John Doe", "john.doe@example.com", "1234-5678-9012-3456"),
#     ("Jane Smith", "jane.smith@example.com", "9876-5432-1098-7654")
# ]
#
# columns = ["name", "email", "credit_card"]
#
# df = spark.createDataFrame(data, columns)
#
# print("Original Data:")
# df.show(truncate=False)
#
# # --------------------------
# # Mask email (replace username part with ****)
# # --------------------------
# masked_email_df = df.withColumn(
#     "email_masked_type1",
#     regexp_replace(col("email"), "^[^@]+",  "****")
# )
#
# # ^ → Indicates the start of the string.
# #
# # [^@] → A negated character class: matches any character except @.
# #
# # + → One or more occurrences of the preceding pattern.
#
# # --------------------------
# # Mask email (replace username part with ****)
# --------------------------
# #
# # --------------------------
# # Mask credit card (replace first 12 digits with *)
# # --------------------------
# masked_df = masked_email_df.withColumn(
#     "credit_card_masked",
#     regexp_replace(col("credit_card"), r"\d{12}", "****-****-****")
# )
#
# # --------------------------
# # Show masked data
# # --------------------------
# print("Masked Data:")
# masked_df.select("name", "email_masked_type1", "credit_card_masked").show(truncate=False)
#
#





from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, concat, substring, length, substring_index

spark = SparkSession.builder.appName("DataMaskingExample").getOrCreate()

data = [
    ("John Doe", "john.doe@example.com", "1234-5678-9012-3456"),
    ("Jane Smith", "jane.smith@example.com", "9876-5432-1098-7654")
]

columns = ["name", "email", "credit_card"]

df = spark.createDataFrame(data, columns)

print("Original Data:")
df.show(truncate=False)

# -----------------------
# Mask email username
# -----------------------
df_masked = df.withColumn("username", substring_index(col("email"), "@", 1)) \
    .withColumn("domain", substring_index(col("email"), "@", -1)) \
    .withColumn("masked_username",
                concat(
                    substring(col("username"), 1, 1),
                    lit("*") * (length(col("username")) - 2),
                    substring(col("username"), -1, 1)
                )) \
    .withColumn("masked_email", concat(col("masked_username"), lit("@"), col("domain")))

# -----------------------
# Mask credit card except last 4 digits
# -----------------------
df_masked = df_masked.withColumn("masked_credit_card",
                                 concat(lit("****-****-****-"), substring(col("credit_card"), -4, 4)))

# -----------------------
# Mask name (optional, keep first letter of each word)
# -----------------------
from pyspark.sql.functions import split, expr, array_join

df_masked = df_masked.withColumn("masked_name",
                                 array_join(expr("transform(split(name, ' '), x -> concat(substring(x,1,1), repeat('*', length(x)-1)))"), " "))

# -----------------------
# Select final masked columns
# -----------------------
df_masked = df_masked.select("masked_name", "masked_email", "masked_credit_card")

print("Masked Data:")
df_masked.show(truncate=False)
