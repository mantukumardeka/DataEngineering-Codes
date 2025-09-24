from pyspark.sql import SparkSession
from setuptools.command.easy_install import CommandSpec

# Create Spark session
spark = SparkSession.builder \
    .appName("MySQL_to_PySpark") \
    .config("spark.jars", "/Users/mantukumardeka/Desktop/DataEngineering/jars/mysql-connector-j-9.4.0/mysql-connector-j-9.4.0.jar") \
    .getOrCreate()

# MySQL connection properties
jdbc_url = "jdbc:mysql://localhost:3306/mkd"  # replace 'mydatabase' with your DB name
table_name = "orders"  # replace with your table name

properties = {
    "user": "root",       # your MySQL username
    "password": "Hadoop@123",  # your MySQL password
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Read table into DataFrame
df = spark.read.jdbc(url=jdbc_url, table="Customer", properties=properties)

# Show data
df.show(5)


## Doing something extra

print("Using  SQL CommandS")

df.createOrReplaceTempView("cust")

df1=spark.sql("select * from cust")

df1.show()




df1.write.jdbc(url=jdbc_url, table="orders_filtered", mode="overwrite", properties=properties)