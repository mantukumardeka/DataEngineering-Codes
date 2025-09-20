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
