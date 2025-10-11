from pyspark.sql import SparkSession

# MySQL JDBC driver
mysql_jar = "/opt/homebrew/Cellar/hive/4.1.0/libexec/lib/mysql-connector-j-9.4.0.jar"

# Spark session with Hive support and JDBC driver
spark = SparkSession.builder \
    .appName("MySQL_to_Hive") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("spark.jars", mysql_jar) \
    .config("spark.driver.extraClassPath", mysql_jar) \
    .config("spark.executor.extraClassPath", mysql_jar) \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Sample data
data = [
    (1, "Alice", "Johnson", "HR", 50000, "2021-05-10", "Female"),
    (2, "Bob", "Smith", "IT", 75000, "2020-08-15", "Male"),
    (3, "Charlie", "Brown", "Finance", 60000, "2019-03-20", "Male"),
    (4, "Diana", "Prince", "IT", 85000, "2021-01-10", "Female"),
    (5, "Eva", "Green", "Marketing", 45000, "2022-07-05", "Female"),
    (6, "Frank", "Adams", "Finance", 70000, "2020-12-11", "Male"),
    (7, "Grace", "Kelly", "HR", 52000, "2018-09-25", "Female"),
    (8, "Hank", "Miller", "IT", 90000, "2023-04-01", "Male"),
    (9, "Ivy", "Harper", "Finance", 58000, "2022-06-20", "Female"),
    (10, "Jack", "Daniels", "HR", 48000, "2021-11-15", "Male"),
    (11, "Kate", "Winslet", "Marketing", 53000, "2020-03-10", "Female"),
    (12, "Liam", "Neeson", "Finance", 75000, "2023-01-20", "Male"),
    (13, "Mia", "Wallace", "HR", 55000, "2019-12-30", "Female"),
    (14, "Nathan", "Drake", "IT", 82000, "2018-02-14", "Male"),
    (15, "Olivia", "Newton", "Marketing", 46000, "2022-09-18", "Female"),
    (2, "Bob", "Smith", "IT", 75000, "2020-08-15", "Male"),
    (7, "Grace", "Kelly", "HR", 52000, "2018-09-25", "Female")
]

columns = ["EMPID","FNAME","LNAME","DEPARTMENT","SALARY","DOJ","GENDER"]

# Create DataFrame
df = spark.createDataFrame(data, columns)

# Remove duplicate EMPID rows
df = df.dropDuplicates(["EMPID"])

df.show()

# Hive database and table
hive_db = "mkd_hive"
hive_table = "uat"

# Create Hive database if not exists
spark.sql(f"CREATE DATABASE IF NOT EXISTS {hive_db}")

# Write DataFrame to Hive table (append mode)
df.write.mode("append").saveAsTable(f"{hive_db}.{hive_table}")

# Verify Hive table
spark.sql(f"SELECT * FROM {hive_db}.{hive_table} LIMIT 10").show()

# Stop Spark session
spark.stop()
