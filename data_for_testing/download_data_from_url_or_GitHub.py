import requests
from pyspark.sql import SparkSession

url = "https://raw.githubusercontent.com/databricks/LearningSparkV2/master/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv"
local_path = "/Users/mantukumardeka/Desktop/DataEngineering/data_for_testing/sf-fire-calls.csv"

r = requests.get(url)
open(local_path, "wb").write(r.content)

spark = SparkSession.builder.appName("FireCalls").getOrCreate()

df = spark.read.option("header", "true").csv(local_path)
df.show(5)
