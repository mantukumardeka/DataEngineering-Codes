
# -- Question 181: Employees Earning More Than Their Managers
# -- Write a SQL query that finds employees who earn more than their managers.
#
#
# -- mysql> select * from Employee_181;
# -- +----+-------+--------+-----------+
# -- | Id | Name  | Salary | ManagerId |
# -- +----+-------+--------+-----------+
# -- |  1 | Joe   |  70000 |         3 |
# -- |  2 | Henry |  80000 |         4 |
# -- |  3 | Sam   |  60000 |      NULL |
# -- |  4 | Max   |  90000 |      NULL |
# -- +----+-------+--------+-----------+


from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("LeetCode 181").getOrCreate()

# Data as list of tuples
employee_data = [
    (1, "Joe",   70000, 3),
    (2, "Henry", 80000, 4),
    (3, "Sam",   60000, None),
    (4, "Max",   90000, None)
]

columns = ["Id", "Name", "Salary", "ManagerId"]

employee_df = spark.createDataFrame(employee_data, columns)

employee_df.show()


from pyspark.sql import functions as F



print("JOIN- Inner join")

new_df=(employee_df.alias("e").join(employee_df.alias("m"), F.col("e.Id")==F.col("m.ManagerId"), "inner") \
        .filter(F.col("e.Salary") > F.col("m.Salary"))) \
    .select(F.col("e.Name")).alias("Employee")
new_df.show()



print("Using DSL ")

employee_df.createOrReplaceTempView("employee_table")

spark.sql("select * from employee_table").show()

spark.sql("select e.name as employee from employee_table e join employee_table m on e.id=m.managerid where e.salary>m.salary").show()



