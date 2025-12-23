
# BRONZE LAYER – Incremental Raw Load
# Strategy
# Since no created_at / updated_at column exists, we use:
# Hash-based incremental detection
# Only new/changed rows are inserted


from pyspark.sql.functions import *
from pyspark.sql.types import *
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


# Source-Loading Data from Source
source_df = spark.table("default.student")

# Add hash for change detection
bronze_df = source_df.withColumn(
    "record_hash",
    sha2(concat_ws("||", *source_df.columns), 256)
).withColumn(
    "ingestion_date",
    current_date()
)

# Create bronze database if not exists
spark.sql("CREATE DATABASE IF NOT EXISTS bronze")

# If bronze table exists → incremental
if spark._jsparkSession.catalog().tableExists("bronze.student_raw"):
    existing_df = spark.table("bronze.student_raw")

    new_df = bronze_df.join(
        existing_df.select("record_hash"),
        on="record_hash",
        how="left_anti"
    )

    new_df.write.mode("append").format("parquet").saveAsTable("bronze.student_raw")

else:
    bronze_df.write.mode("overwrite").format("parquet").saveAsTable("bronze.student_raw")