import requests
from pyspark.sql import SparkSession

# Correct RAW JSON URL
url = "https://raw.githubusercontent.com/mohankrishna02/interview-scenerios-spark-sql/master/Datasets/scen20.json"

local_path = "/Users/mantukumardeka/Desktop/DataEngineering/data_for_testing/complex1.json"

print("Downloading JSON...")
r = requests.get(url)

# Save JSON file
with open(local_path, "wb") as f:
    f.write(r.content)

print("Download complete.")

# Start Spark
spark = SparkSession.builder.appName("ScenarioData").getOrCreate()

# Read JSON
df = df = spark.read.format("json").option("multiline", "true").load(f"file://{local_path}")

print("Schema:")

df.show()
df.printSchema()

from pyspark.sql.functions import *

compdf = df.select(
    col("code"),
    col("commentCount"),
    col("createdAt"),
    col("description"),
    col("feedsComment"),
    col("id"),
    col("imagePaths"),
    col("images"),
    col("isdeleted"),
    col("lat"),
    struct(col("dislikes"), col("likes"), col("userAction")).alias("likeDislike"),
    col("lng"),
    col("location"),
    col("mediatype"),
    col("msg"),
    array(
        struct(
            col("createAt"),
            col("description"),
            col("id"),
            col("likeCount"),
            col("mediatype"),
            col("name"),
            col("place"),
            col("url")
        ).alias("element")
    ).alias("multiMedia"),
    col("name"),
    col("profilePicture"),
    col("title"),
    col("userId"),
    col("videoUrl"),
    col("totalFeed")
)

compdf.printSchema()

compdf.show()
