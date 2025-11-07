from pyspark.sql import SparkSession

# ------------------------------
# Step 1: Spark session with Hive & MySQL Metastore support
# ------------------------------
mysql_jar = "/opt/homebrew/Cellar/hive/4.1.0/libexec/lib/mysql-connector-j-9.4.0.jar"

spark = SparkSession.builder \
    .appName("LocalCSV_to_Hive_Partitioned") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("spark.jars", mysql_jar) \
    .config("spark.driver.extraClassPath", mysql_jar) \
    .config("spark.executor.extraClassPath", mysql_jar) \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ------------------------------
# Step 2: Read CSV from local Mac path
# ------------------------------
df = spark.read.format("csv") \
    .option("header", "true") \
    .load("file:///Users/mantukumardeka/Desktop/data/worker_data.csv")

print("✅ Local CSV loaded successfully:")
df.show()

# ------------------------------
# Step 3: Create Hive database
# ------------------------------
hive_db = "mkd_hive"
hive_table = "worker_table_partitioned"

spark.sql(f"CREATE DATABASE IF NOT EXISTS {hive_db}")
spark.sql(f"USE {hive_db}")

# ------------------------------
# Step 4: Write DataFrame to Hive table (Partitioned by Department)
# ------------------------------
df.write.mode("overwrite") \
    .format("hive") \
    .partitionBy("DEPARTMENT") \
    .saveAsTable(f"{hive_db}.{hive_table}")

print(f"✅ Data successfully written to Hive table (partitioned by DEPARTMENT): {hive_db}.{hive_table}")

# ------------------------------
# Step 5: Verify data from Hive
# ------------------------------
print("🔍 Verifying data from Hive table:")
spark.sql(f"SELECT * FROM {hive_db}.{hive_table} LIMIT 10").show()

# ------------------------------
# Step 6: Check partitions
# ------------------------------
print("🧩 Hive partitions:")
spark.sql(f"SHOW PARTITIONS {hive_db}.{hive_table}").show()
