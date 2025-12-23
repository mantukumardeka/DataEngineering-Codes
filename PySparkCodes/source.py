



from pyspark.sql import SparkSession

# ------------------------------
# Step 1: Configure Spark with Hive support and MySQL metastore
# ------------------------------
mysql_jar = "/opt/homebrew/Cellar/hive/4.1.0/libexec/lib/mysql-connector-j-9.4.0.jar"

spark = SparkSession.builder \
    .appName("LocalCSV_to_Hive") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("spark.jars", mysql_jar) \
    .config("spark.driver.extraClassPath", mysql_jar) \
    .config("spark.executor.extraClassPath", mysql_jar) \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ------------------------------
# Step 2: Read CSV file from local MacBook path
# ------------------------------
df = spark.read.format("csv") \
    .option("header", "true") \
    .load("file:///Users/mantukumardeka/Desktop/Attendance_Prediction.csv")

print("✅ Local CSV loaded successfully:")

df.limit(10)
df.show()

# ------------------------------
# Step 3: Define Hive database and table
# ------------------------------
hive_db = "default"
hive_table = "student"

# Create Hive database if it doesn’t exist
spark.sql(f"CREATE DATABASE IF NOT EXISTS {hive_db}")

# Switch to the Hive database
spark.sql(f"USE {hive_db}")

# ------------------------------
# Step 4: Write DataFrame to Hive table
# ------------------------------
# If you want Hive to manage the table location automatically:
df.write.mode("overwrite").saveAsTable(f"{hive_db}.{hive_table}")

# OR, if you want to specify an explicit HDFS location:
# df.write.mode("overwrite").option("path", "hdfs://localhost:9000/mkd/worker_table").saveAsTable(f"{hive_db}.{hive_table}")

print(f"✅ Data successfully written to Hive table: {hive_db}.{hive_table}")

# ------------------------------
# Step 5: Verify from Hive
# ------------------------------
print("🔍 Verifying data from Hive table:")
spark.sql(f"SELECT * FROM {hive_db}.{hive_table} LIMIT 5").show()
