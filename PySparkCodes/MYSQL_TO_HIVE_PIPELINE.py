from pyspark.sql import SparkSession

# ------------------------------
# Step 1: Create Spark Session with Hive support
# ------------------------------
spark = SparkSession.builder \
    .appName("MySQL_to_Hive") \
    .config("spark.jars", "/Users/mantukumardeka/Desktop/DataEngineering/jars/mysql-connector-j-9.4.0/mysql-connector-j-9.4.0.jar") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .enableHiveSupport() \
    .getOrCreate()

# ------------------------------
# Step 2: MySQL connection properties
# ------------------------------
jdbc_url = "jdbc:mysql://localhost:3306/mkd"
table_name = "Customer"

properties = {
    "user": "root",
    "password": "Hadoop@123",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# ------------------------------
# Step 3: Read MySQL table into DataFrame
# ------------------------------
df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=properties)
df.show()

# ------------------------------
# Step 4: Optional transformations / filters
# Example: filter customers with id > 100
# ------------------------------
# df_filtered = df.filter(df["customer_id"] > 1)


# ------------------------------
# Step 5: Write to Hive
# ------------------------------
hive_db = "mkd_hive"
hive_table = "customer_filtered"

# Create Hive database if not exists
spark.sql(f"CREATE DATABASE IF NOT EXISTS {hive_db}")

# Write filtered DataFrame to Hive table
#df_filtered.write.mode("overwrite").saveAsTable(f"{hive_db}.{hive_table}")

#df.write.mode("overwrite").saveAsTable(f"{hive_db}.{hive_table}")

df.write.mode("append").saveAsTable(f"{hive_db}.{hive_table}")

# ------------------------------
# Step 6: Verify Hive table
# ------------------------------
spark.sql(f"SELECT * FROM {hive_db}.{hive_table} LIMIT 10").show()

# ------------------------------
# Step 7: Stop Spark session
# ------------------------------
spark.stop()
