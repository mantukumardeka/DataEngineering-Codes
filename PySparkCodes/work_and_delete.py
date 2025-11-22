import requests
from pyspark.sql import SparkSession

# Correct RAW JSON URL
url = "https://raw.githubusercontent.com/mohankrishna02/interview-scenerios-spark-sql/master/Datasets/scen.json"

local_path = "/Users/mantukumardeka/Desktop/DataEngineering/data_for_testing/complex2.json"

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


from pyspark.sql.functions import *
print("Data:")
df.show(5, truncate=False)
df.printSchema()
finaldf = df.withColumn("multiMedia", explode(col("multiMedia"))).withColumn("dislikes",
                                                                             expr("likeDislike.dislikes")).withColumn(
    "likes", expr("likeDislike.likes")).withColumn("userAction", expr("likeDislike.userAction")).withColumn("createAt",
                                                                                                            expr(
                                                                                                                "multiMedia.createAt")).withColumn(
    "description", expr("multiMedia.description")).withColumn("id", expr("multiMedia.id")).withColumn("likeCount", expr(
    "multiMedia.likeCount")).withColumn("mediatype", expr("multiMedia.mediatype")).withColumn("name", expr(
    "multiMedia.name")).withColumn("place", expr("multiMedia.place")).withColumn("url", expr("multiMedia.url")).drop(
    "likeDislike", "multiMedia")
print("flat Schema")
finaldf.printSchema()
finaldf.show()