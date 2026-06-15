import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# 1. Fetch data from your API
response = requests.get("http://localhost:8000/employees")
data = response.json()

# 2. Initialize Spark
spark = SparkSession.builder.appName("APPS").getOrCreate()

# 3. Create DataFrame
df = spark.createDataFrame(data)

# 4. FIX: Changed "emp_id" to "id" to match your API's schema
df.select("id", "name", "salary", "loc").show()
