import requests
from pyspark.sql import SparkSession

# Correct RAW JSON URL
url = "https://raw.githubusercontent.com/mohankrishna02/interview-scenerios-spark-sql/master/Datasets/scen.json"

local_path = "/Users/mantukumardeka/Desktop/DataEngineering/data_for_testing/complex.json"

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
df.printSchema()

print("Data:")
df.show(5, truncate=False)
