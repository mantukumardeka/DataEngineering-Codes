from pyspark.sql import SparkSession

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("HiveCompatibility") \
    .config("spark.sql.warehouse.dir", "/Users/mantukumardeka/Desktop/DataEngineering/warehouse") \
    .enableHiveSupport() \
    .getOrCreate()


# Check if Hive is working
spark.sql("SHOW DATABASES").show()

# Key Points:
#
# enableHiveSupport()
#
# Enables HiveQL features like CREATE TABLE, SHOW DATABASES, INSERT INTO etc.
#
# Lets Spark connect with Hive metastore.
#
# Hive Metastore
#
# Make sure Hive is installed and hive-site.xml is in your SPARK_CONF_DIR (or added via .config()).
#
# Otherwise, Spark will create its own warehouse at the path you specify in spark.sql.warehouse.dir.
#
# Running Queries
# Once Hive support is enable



# Create a database
spark.sql("CREATE DATABASE IF NOT EXISTS mkd")

# Use the database
spark.sql("USE mkd")

# Create a Hive table
spark.sql("""
    CREATE TABLE IF NOT EXISTS employees (
        id INT,
        name STRING,
        dept STRING
    )
    USING hive
""")

# Insert sample data
spark.sql("INSERT INTO employees VALUES (1, 'Alice', 'HR'), (2, 'Bob', 'IT')")

# Query the table
spark.sql("SELECT * FROM employees").show()
