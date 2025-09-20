
from pyspark.sql import SparkSession


#  Need to enable Hive Support
spark = SparkSession.builder \
    .appName("PartitionAndBucketingExample") \
    .enableHiveSupport() \
    .getOrCreate()


# Sample dataset of 20 rows
data = [
    ("Emp01", "HR", 3000),
    ("Emp02", "HR", 3200),
    ("Emp03", "HR", 3500),
    ("Emp04", "HR", 2800),
    ("Emp05", "Sales", 4000),
    ("Emp06", "Sales", 4200),
    ("Emp07", "Sales", 4100),
    ("Emp08", "Sales", 3900),
    ("Emp09", "IT", 5000),
    ("Emp10", "IT", 5200),
    ("Emp11", "IT", 5100),
    ("Emp12", "IT", 4800),
    ("Emp13", "Finance", 4500),
    ("Emp14", "Finance", 4600),
    ("Emp15", "Finance", 4700),
    ("Emp16", "Finance", 4400),
    ("Emp17", "Admin", 2500),
    ("Emp18", "Admin", 2600),
    ("Emp19", "Admin", 2700),
    ("Emp20", "Admin", 2800),
]

columns = ["emp_id", "department", "salary"]
df = spark.createDataFrame(data, columns)

print("Original Data:")
df.show()

# Write dataset partitioned by department
# . Notes / Best Practices
#
# Use overwrite carefully → it deletes existing data.
#
# append → good for ETL pipelines to add new batch data.
#
# ignore → useful when jobs run multiple times and you don’t want errors.
#
# Default mode is error → always check if path exists to avoid exceptions.

try:
    df.write.mode("overwrite")\
    .partitionBy("department") \
    .parquet("/Users/mantukumardeka/Desktop/delete")

    print("Data written successfully.")

except Exception as e:
    print("Data is not written.May be data is already there",str(e))

except Exception as ed:
    print("Data is already there",str(ed))
except   Exception as f:
    print("Something went wrong- Fight be folder is not there",str(f))
finally:
    print("All done bye")


#BUECKING EXAMPLE

# Save dataset bucketed by department (4 buckets)
df.write.mode("overwrite") \
    .bucketBy(4, "department") \
    .sortBy("department") \
    .saveAsTable("bucketed_employees")


# Read partitioned data

# partitioned_df = spark.read.parquet("/Users/mantukumardeka/Desktop/delete")
# print("Partitioned Data:")
# partitioned_df.show()
#
# # Read bucketed data (as Hive table)
bucketed_df = spark.table("bucketed_employees")
print("Bucketed Data:")
bucketed_df.show()
