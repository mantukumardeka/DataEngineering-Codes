# -- Question 184-
#
# --  Write a solution to find employees who have the highest salary in each of the departments.
# -- Return the result table in any order.


# mysql> select * from department_184;
# +----+-------+
# | id | name  |
# +----+-------+
# |  1 | IT    |
# |  2 | Sales |
# +----+-------+
# 2 rows in set (0.04 sec)
#
# mysql> select * from employee_184;
# +----+-------+--------+--------------+
# | id | name  | salary | departmentId |
# +----+-------+--------+--------------+
# |  1 | Joe   |  70000 |            1 |
# |  2 | Jim   |  90000 |            1 |
# |  3 | Henry |  80000 |            2 |
# |  4 | Sam   |  60000 |            2 |
# |  5 | Max   |  90000 |            1 |
# +----+-------+--------+--------------+
#
# -- Output:
# -- +------------+----------+--------+
# -- | Department | Employee | Salary |
# -- +------------+----------+--------+
# -- | IT         | Jim      | 90000  |
# -- | Sales      | Henry    | 80000  |
# -- | IT         | Max      | 90000  |
# -- +------------+----------+--------+



from pyspark.sql import SparkSession

from PySparkCodes.leetcode_178 import windowspace

spark=SparkSession.builder.appName("Leetcode_184").getOrCreate()

department_data = [
    (1, "IT"),
    (2, "Sales")
]

department_schema = ["id", "name"]

department_df=spark.createDataFrame(department_data,department_schema)

department_df.show()

employee_data = [
    (1, "Joe", 70000, 1),
    (2, "Jim", 90000, 1),
    (3, "Henry", 80000, 2),
    (4, "Sam", 60000, 2),
    (5, "Max", 90000, 1)
]

employee_schema = ["id", "name", "salary", "departmentId"]

employee_df=spark.createDataFrame(employee_data,employee_schema)
employee_df.show()

from pyspark.sql import functions
from pyspark.sql.window import Window

from pyspark.sql import functions as F

# windowspace=Window.partitionBy(department_df.name).orderBy(employee_df.salary.desc)
#
#
# max_df=employee_df.join(department_df,employee_df.departmentId==department_df.id,"inner").withColumn("")
#
# max_df.show()

joined_df = employee_df.join(
    department_df,
    employee_df.departmentId == department_df.id,
    "inner"
).select(
    department_df.name.alias("Department"),
    employee_df.name.alias("Employee"),
    employee_df.salary.alias("Salary")
)

# Window to rank salaries within each department
windowSpec = Window.partitionBy("Department").orderBy(F.col("Salary").desc())

ranked_df = joined_df.withColumn("rank", F.dense_rank().over(windowSpec))

# Filter highest salary per department
result_df = ranked_df.filter(F.col("rank") == 1).drop("rank")

result_df.show()

