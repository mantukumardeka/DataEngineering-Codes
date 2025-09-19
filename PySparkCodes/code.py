from pyspark.sql import SparkSession

# 1. Create Spark session
spark = SparkSession.builder \
    .appName("SelectFromEmp") \
    .getOrCreate()

# 2. Sample emp data (id, name, salary, dept)
data = [
    (1, "John", 50000, "HR"),
    (2, "Mary", 60000, "IT"),
    (3, "Steve", 55000, "Finance"),
    (4, "Anna", 75000, "IT"),
    (5, "Mike", 45000, "HR")
]

columns = ["emp_id", "emp_name", "salary", "dept"]

# 3. Create DataFrame
empDF = spark.createDataFrame(data, columns)

# 4. Register as SQL temporary view
empDF.createOrReplaceTempView("emp")

# 5. Run SELECT query
result = spark.sql("""
    SELECT emp_id, emp_name, salary
    FROM emp
    WHERE dept = 'IT'
""")

# 6. Show result
result.show()
