from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("FoodData").getOrCreate()

# -----------------------------
# DataFrame 1 : Food Items
# -----------------------------
df_food = spark.createDataFrame(
    [
        (1, "Veg Biryani"),
        (2, "Veg Fried Rice"),
        (3, "Kaju Fried Rice"),
        (4, "Chicken Biryani"),
        (5, "Chicken Dum Biryani"),
        (6, "Prawns Biryani"),
        (7, "Fish Birayani"),
    ],
    ["food_id", "food_item"]
)

# -----------------------------
# DataFrame 2 : Ratings
# -----------------------------
df_rating = spark.createDataFrame(
    [
        (1, 5),
        (2, 3),
        (3, 4),
        (4, 4),
        (5, 5),
        (6, 4),
        (7, 4),
    ],
    ["food_id", "rating"]
)

df_food.show()
df_rating.show()

