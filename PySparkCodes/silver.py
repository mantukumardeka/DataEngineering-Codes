## SILVER LAYER – Clean & Transform
        ## Transformations
        ## Deduplication
        ## Type casting
        ## Standardization
        ## Business flags


from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

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

spark.sql("CREATE DATABASE IF NOT EXISTS silver")

bronze_df = spark.table("bronze.student_raw")

# Deduplicate
window_spec = Window.partitionBy(
    "student_id", "age", "gender", "course", "year"
).orderBy(col("ingestion_date").desc())

silver_df = bronze_df.withColumn(
    "row_num", row_number().over(window_spec)
).filter(col("row_num") == 1).drop("row_num")

# Standardization & cleansing
silver_df = silver_df \
    .withColumn("gender", lower(trim(col("gender")))) \
    .withColumn("course", upper(trim(col("course")))) \
    .withColumn("attendance_flag", col("attendance").cast("int")) \
    .withColumn("study_hours", col("study_hours").cast("double")) \
    .withColumn("sleep_hours", col("sleep_hours").cast("double")) \
    .withColumn(
        "attendance_status",
        when(col("attendance") == 1, "PRESENT").otherwise("ABSENT")
    )

silver_df.write.mode("overwrite").format("parquet").saveAsTable("silver.student_clean")



