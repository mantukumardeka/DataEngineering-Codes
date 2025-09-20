from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

# Create SparkSession
spark = SparkSession.builder \
    .appName("ExceptionHandlingExample") \
    .getOrCreate()

try:
    # Try reading a file
    df = spark.read.csv("/Users/mantukumardeka/Desktop/data/worker_data.csv", header=True, inferSchema=True)
    print("✅ File loaded successfully!")

    try:
        # Transformation: Assume column 'salary' exists
        df_transformed = df.withColumn("salary_double", df["salary"] * 2)
        df_transformed.show(5)

    except Exception as e:
        print("⚠️ Error while transforming data:", str(e))

except AnalysisException as ae:
    print("❌ File not found or schema issue:", str(ae))

except Exception as e:
    print("❌ Unexpected error occurred:", str(e))

finally:
    print("ℹ️ Job finished (with or without errors).")
    spark.stop()




# print(" What this does:")
#
# Outer Try Block:
# Handles file read errors (e.g., file not found, schema mismatch).
#
# Inner Try Block:
# Handles transformation errors (e.g., column not found, invalid operations).
#
# Specific Exception Handling:
#
# AnalysisException → Spark-specific errors (like missing table, wrong path).
#
# Generic Exception → Catches all other Python errors.
#
# Finally Block:
# Ensures resources (like spark.stop()) are cleaned up.

