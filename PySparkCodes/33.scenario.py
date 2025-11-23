from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, max as max_

spark = SparkSession.builder.appName("DiscountTours").getOrCreate()

# --------------------------
# Family Table (tab33_1)
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("tab33").getOrCreate()

# -----------------------------
# DataFrame 1
# -----------------------------
familydf = spark.createDataFrame(
    [
        ("c00dac11bde74750b...", "Alex Thomas", 9),
        ("eb6f2d3426694667a...", "Chris Gray", 2),
        ("3f7b5b8e835d4e1c8...", "Emily Johnson", 4),
        ("9a345b079d9f4d3ca...", "Michael Brown", 6),
        ("e0a5f57516024de2a...", "Jessica Wilson", 3)
    ],
    ["idf", "name", "family_size"]
)

familydf.show(truncate=False)

# -----------------------------
# DataFrame 2
# -----------------------------
countrydf = spark.createDataFrame(
    [
        ("023fd23615bd4ff4b...", "Bolivia", 2, 4),
        ("be247f73de0f4b2d8...", "Cook Islands", 4, 8),
        ("3e85ab80a6f84ef3b...", "Brazil", 4, 7),
        ("e571e164152c4f7c8...", "Australia", 5, 9),
        ("f35a7bb7d44342f7a...", "Canada", 3, 5),
        ("a1b5a4b5fc5f46f89...", "Japan", 10, 12)
    ],
    ["id", "name", "min_size", "max_size"]
)

countrydf.show(truncate=False)

# --------------------------
# Join using family_size between min_size and max_size
# --------------------------
join_df = familydf.join(
    countrydf,
    (familydf.family_size >= countrydf.min_size) & (familydf.family_size <= countrydf.max_size),
    "inner"
)

# --------------------------
# Count tours per family
# --------------------------
tour_count_df = join_df.groupBy("idf").agg(
    count("*").alias("tour_count")
)

# --------------------------
# Get the max tours any family can take
# --------------------------
result = tour_count_df.agg(
    max_("tour_count").alias("max_discount_tours")
)

result.show()

print("USING SQL")

familydf.createTempView("tab33_1")
countrydf.createTempView("tab33_2")

spark.sql("select  max(total_number_country ) from (select a.name , count(*) as total_number_country from tab33_1 a join tab33_2  b on a.family_size between b.min_size and b.max_size group by a.name) a").show()