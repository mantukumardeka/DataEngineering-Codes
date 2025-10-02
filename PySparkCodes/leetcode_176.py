

# Get the @nd highest salary-
# Input:
# Employee table:
# +----+--------+
# | id | salary |
# +----+--------+
# | 1  | 100    |
# | 2  | 200    |
# | 3  | 300    |
# +----+--------+
# Output:
# +---------------------+
# | SecondHighestSalary |
# +---------------------+
# | 200                 |
# +---------------------+


from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("LeetCode 176").getOrCreate()

data=[("1","100"),("2","200`"),("3","300")]

employee_df=spark.createDataFrame(data=data,schema=["id","salary"])
employee_df.show()


print("2nd Highest Salary using DSL - (LIMIT and OFFSET)")


employee_df.select("salary").limit(2).offset(1).show()


print("Using Window Function")

from pyspark.sql.window import Window
from pyspark.sql import functions
from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank


print("Getting RANK")
windowspace=Window.orderBy(functions.col("salary").desc())

rankdf=employee_df.withColumn("rank",dense_rank().over(windowspace))

rankdf.show()

print("Filtering the RANK")

secondhighest=rankdf.filter(functions.col("rank")==2).select("salary")



secondhighest.show()





