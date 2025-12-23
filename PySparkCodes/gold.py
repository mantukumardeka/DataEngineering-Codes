from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# ------------------------------------------------
# Spark Session (Hive + MySQL Metastore)
# ------------------------------------------------
mysql_jar = "/opt/homebrew/Cellar/hive/4.1.0/libexec/lib/mysql-connector-j-9.4.0.jar"

spark = SparkSession.builder \
    .appName("Gold_Layer_Star_Schema_SCD2") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("spark.jars", mysql_jar) \
    .config("spark.driver.extraClassPath", mysql_jar) \
    .config("spark.executor.extraClassPath", mysql_jar) \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ------------------------------------------------
# Ensure databases
# ------------------------------------------------
spark.sql("CREATE DATABASE IF NOT EXISTS gold")

# ------------------------------------------------
# Load SILVER data
# ------------------------------------------------
silver_df = spark.table("silver.student_clean")

# =================================================
# 1️⃣ DIMENSION: STUDENT (SCD TYPE 2)
# =================================================

incoming_df = silver_df.select(
    col("student_id").cast("string"),
    "age",
    "gender",
    "parent_education",
    "hostel_resident",
    "internet_access"
).dropDuplicates()

incoming_df = incoming_df.withColumn(
    "attr_hash",
    sha2(concat_ws(
        "||",
        col("age"),
        col("gender"),
        col("parent_education"),
        col("hostel_resident"),
        col("internet_access")
    ), 256)
)

table_exists = spark._jsparkSession.catalog().tableExists("gold.dim_student")

# ---------------- FIRST LOAD ----------------
if not table_exists:

    final_dim = incoming_df \
        .withColumn("student_sk", monotonically_increasing_id()) \
        .withColumn("start_date", current_date()) \
        .withColumn("end_date", lit("9999-12-31").cast("date")) \
        .withColumn("is_current", lit(1))

    final_dim.write.mode("overwrite").format("parquet") \
        .saveAsTable("gold.dim_student")

# -------------- INCREMENTAL (SCD-2) ----------
else:
    dim_df = spark.table("gold.dim_student")
    current_dim = dim_df.filter(col("is_current") == 1)

    joined_df = incoming_df.alias("i").join(
        current_dim.alias("d"),
        on="student_id",
        how="left"
    )

    changed_df = joined_df.filter(
        col("d.student_id").isNotNull() &
        (col("i.attr_hash") != col("d.attr_hash"))
    )

    new_df = joined_df.filter(col("d.student_id").isNull())

    expired_df = current_dim.alias("d").join(
        changed_df.select("student_id"),
        "student_id"
    ).withColumn(
        "end_date", date_sub(current_date(), 1)
    ).withColumn(
        "is_current", lit(0)
    )

    new_versions_df = changed_df.select("i.*") \
        .unionByName(new_df.select("i.*")) \
        .withColumn("student_sk", monotonically_increasing_id()) \
        .withColumn("start_date", current_date()) \
        .withColumn("end_date", lit("9999-12-31").cast("date")) \
        .withColumn("is_current", lit(1))

    unchanged_df = current_dim.join(
        changed_df.select("student_id"),
        "student_id",
        "left_anti"
    )

    final_dim = unchanged_df \
        .unionByName(expired_df) \
        .unionByName(new_versions_df)

    # 🔑 BREAK READ–WRITE LINEAGE
    final_dim = final_dim.cache()
    final_dim.count()

    final_dim.write.mode("overwrite").format("parquet") \
        .saveAsTable("gold.dim_student")

# =================================================
# 2️⃣ DIMENSION: COURSE
# =================================================
course_dim = silver_df.select(
    "course",
    "year",
    "class_type"
).dropDuplicates() \
 .withColumn("course_sk", monotonically_increasing_id())

course_dim.write.mode("overwrite").format("parquet") \
    .saveAsTable("gold.dim_course")

# =================================================
# 3️⃣ DIMENSION: DATE
# =================================================
date_dim = silver_df.select(
    col("ingestion_date").alias("date")
).dropDuplicates() \
 .withColumn("date_sk", date_format(col("date"), "yyyyMMdd").cast("int")) \
 .withColumn("year", year(col("date"))) \
 .withColumn("month", month(col("date"))) \
 .withColumn("day", dayofmonth(col("date")))

date_dim.write.mode("overwrite").format("parquet") \
    .saveAsTable("gold.dim_date")

# =================================================
# 4️⃣ FACT TABLE: ATTENDANCE
# =================================================
student_dim = spark.table("gold.dim_student") \
    .filter(col("is_current") == 1)

fact_df = silver_df.alias("s") \
    .join(student_dim.alias("sd"), "student_id") \
    .join(course_dim.alias("cd"), ["course", "year", "class_type"]) \
    .join(date_dim.alias("dd"), col("s.ingestion_date") == col("dd.date")) \
    .select(
        col("sd.student_sk"),
        col("cd.course_sk"),
        col("dd.date_sk"),
        col("s.attendance_flag"),
        col("s.study_hours"),
        col("s.sleep_hours"),
        col("s.travel_time_minutes"),
        col("s.weather")
    )

fact_df.write.mode("overwrite").format("parquet") \
    .saveAsTable("gold.fact_attendance")
