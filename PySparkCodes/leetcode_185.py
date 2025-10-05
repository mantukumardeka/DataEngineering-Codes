
# -- Question 185:
# -- Write a solution to find the employees who are high earners  in each of the departments.
# -- Return the result table  in any order.
# -- Department wise top three in each department
#
#
# --  OUTPUT:
#
# -- +------------+----------+--------+
# -- | Department | Employee | Salary |
# -- +------------+----------+--------+
# -- | IT         | Max      | 90000  |
# -- | IT         | Joe      | 85000  |
# -- | IT         | Randy    | 85000  |
# -- | IT         | Will     | 70000  |
# -- | Sales      | Henry    | 80000  |
# -- | Sales      | Sam      | 60000  |
# -- +------------+----------+--------+

from pyspark.sql import SparkSession

from PySparkCodes.leetcode_180 import windowSpec

spark=SparkSession.builder.appName("leetcode_185").getOrCreate()


department_data = [
    (1, "IT"),
    (2, "Sales")
]

department_schema = ["id", "name"]

department_df=spark.createDataFrame(department_data,department_schema)

department_df.show()

employee_data = [
    (1, "Joe", 85000, 1),
    (2, "Henry", 80000, 2),
    (3, "Sam", 60000, 2),
    (4, "Max", 90000, 1),
    (5, "Janet", 69000, 1),
    (6, "Randy", 85000, 1),
    (7, "Will", 70000, 1)
]

employee_schema = ["id", "name", "salary", "departmentId"]

employee_df=spark.createDataFrame(employee_data,employee_schema)

employee_df.show()

print("Joining Table")

join_df=(employee_df.join(department_df,employee_df.departmentId==department_df.id ,"inner"))\
    .select(department_df.name.alias("Department"),employee_df.name.alias("Employee"),employee_df.salary.alias("Salary"))


join_df.show()

print("Winowing it")

from pyspark.sql import functions as F
from pyspark.sql.window import Window


windowSpec=Window.partitionBy("Department").orderBy(F.col("Salary").desc())

rank_df=join_df.withColumn("ranks", F.dense_rank().over(windowSpec))

rank_df.show()


final_df=rank_df.filter(F.col("ranks")<=3).drop("ranks")

final_df.show()


