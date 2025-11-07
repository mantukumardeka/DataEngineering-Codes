from pyspark.sql import SparkSession

# MySQL JDBC driver
mysql_jar = "/opt/homebrew/Cellar/hive/4.1.0/libexec/lib/mysql-connector-j-9.4.0.jar"

# SparkSession with Hive support
spark = SparkSession.builder \
    .appName("Hive_External_Table_Partitioned") \
    .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse") \
    .config("spark.jars", mysql_jar) \
    .config("spark.driver.extraClassPath", mysql_jar) \
    .config("spark.executor.extraClassPath", mysql_jar) \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ---------------------------
# Step 1: Read local CSV
# ---------------------------
df = spark.read.format("csv") \
    .option("header", "true") \
    .load("file:///Users/mantukumardeka/Desktop/data/worker_data.csv")

df.show()

# ---------------------------
# Step 2: Create Hive Database
# ---------------------------
hive_db = "mkd_hive"
spark.sql(f"CREATE DATABASE IF NOT EXISTS {hive_db}")

# ---------------------------
# Step 3: Define HDFS external table location
# ---------------------------
hdfs_path = "hdfs://localhost:9000/mkd/worker_partitioned"

# ---------------------------
# Step 4: Create External Table in Hive
# ---------------------------
spark.sql(f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {hive_db}.worker_partitioned (
        worker_id INT,
        worker_name STRING,
        salary DOUBLE
    )
    PARTITIONED BY (department STRING)
    STORED AS PARQUET
    LOCATION '{hdfs_path}'
""")

print("✅ External partitioned table created in Hive")

# ---------------------------
# Step 5: Write data into HDFS partitioned by department
# ---------------------------
df.write \
  .mode("overwrite") \
  .format("parquet") \
  .partitionBy("department") \
  .save(hdfs_path)

print("✅ Data written to HDFS:", hdfs_path)

# ---------------------------
# Step 6: Refresh Hive metadata to detect new partitions
# ---------------------------
spark.sql(f"MSCK REPAIR TABLE {hive_db}.worker_partitioned")

print("✅ Hive partitions refreshed successfully")

