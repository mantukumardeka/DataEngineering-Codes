from xml.sax.handler import feature_namespaces

from pyspark.sql import *
from pyspark.sql.functions import *



spark = SparkSession.builder.appName("Hello").getOrCreate()

# data=[ (1, "Alice", "Johnson", "HR", 50000, "2021-05-10", "Female"),
#   (2, "Bob", "Smith", "IT", 75000, "2020-08-15", "Male"),
#   (3, "Charlie", "Brown", "Finance", 60000, "2019-03-20", "Male"),
#   (4, "Diana", "Prince", "IT", 85000, "2021-01-10", "Female"),
#   (5, "Eva", "Green", "Marketing", 45000, "2022-07-05", "Female"),
#   (6, "Frank", "Adams", "Finance", 70000, "2020-12-11", "Male"),
#   (7, "Grace", "Kelly", "HR", 52000, "2018-09-25", "Female"),
#   (8, "Hank", "Miller", "IT", 90000, "2023-04-01", "Male"),
#   (9, "Ivy", "Harper", "Finance", 58000, "2022-06-20", "Female"),
#   (10, "Jack", "Daniels", "HR", 48000, "2021-11-15", "Male"),
#   (11, "Kate", "Winslet", "Marketing", 53000, "2020-03-10", "Female"),
#   (12, "Liam", "Neeson", "Finance", 75000, "2023-01-20", "Male"),
#   (13, "Mia", "Wallace", "HR", 55000, "2019-12-30", "Female"),
#   (14, "Nathan", "Drake", "IT", 82000, "2018-02-14", "Male"),
#   (15, "Olivia", "Newton", "Marketing", 46000, "2022-09-18", "Female"),
#   (2, "Bob", "Smith", "IT", 75000, "2020-08-15", "Male"),
#   (7, "Grace", "Kelly", "HR", 52000, "2018-09-25", "Female")
#
#
# ]
#
# schema=["empid","fname","lname","dep","sal","doj","gender"]
#
# df = spark.createDataFrame(data, schema)
#
#
# print("Using Spark DataFrame")
#
#
#
# print("SPARK SQL")
# df.createOrReplaceTempView("emp")
#
# data =[
#   ("East", "A", 100),
#   ("East", "B", 150),
#   ("West", "A", 200),
#   ("West", "B", 250)
# ]
#
# df =spark.createDataFrame(data,["region", "product", "sales"])
#
# df.show()
#
# print("PIVOT")
#
# df.groupby("region").pivot("product").sum().show()

# data = [
#     ("Alice", "Math", 85),
#     ("Alice", "Science", 90),
#     ("Alice", "History", 78),
#     ("Bob", "Math", 60),
#     ("Bob", "Science", 70),
#     ("Bob", "History", 80),
#     ("Cathy", "Math", 95),
#     ("Cathy", "Science", 88),
# ]
#
# columns = ["student", "subject", "marks"]
#
# df = spark.createDataFrame(data, columns)
# df.show()
#
#
# print("PIVOT")
#
# df.groupby("student").pivot("subject").sum("marks").show()
#
# df.groupby("student").pivot("subject").avg("marks").show()
#
#
# df.groupby("student").pivot("subject").max("marks").show()

#
# data=[(1, "id","1001"),
#       (1, "name","adi"),
#       (2, "id","1002"),
#       (2, "name","vas")]
#
# schema=["pid","keys","values"]
#
# df = spark.createDataFrame(data, schema)
#
# df.show()
#
# print("PIVOT")
#
# df.groupby("pid").pivot("keys").agg(first("values").alias("first")).show()
#
# # df.groupBy("pid").pivot("keys").agg(first("values")).show()
#
# data=[  ("A", "Laptop", 1000),
#     ("A", "Phone", 800),
#     ("A", "Laptop", 1500),
#     ("B", "Laptop", 1200),
#     ("B", "Phone", 600),
#     ("B", "Phone", 1000)
# ]
#
# schema=["Category", "Product", "Sales"]
#
# df=spark.createDataFrame(data, schema)
#
# df.show()
#
# print("PIVOT")
#
# print("FIRST")
#
# df.groupby("Category").pivot("Product").agg(first("Sales")).show()
#
# print("SUM")
#
# df.groupby("Category").pivot(("Product")).agg(sum("Sales")).show()
#
# print("MIN")
#
# df.groupby("Category").pivot(("Product")).agg(min("Sales")).show()
#
# print("MAX")
#
# df.groupby("Category").pivot(("Product")).agg(max("Sales")).show()
#
#
# print("AVG")
#
# df.groupby("Category").pivot(("Product")).agg(avg("Sales")).show()
#
#



# data = [
#     ("Alice", "Math", 85),
#     ("Alice", "Science", 90),
#     ("Alice", "History", 78),
#     ("Bob", "Math", 60),
#     ("Bob", "Science", 70),
#     ("Bob", "History", 80),
#     ("Cathy", "Math", 95),
#     ("Cathy", "Science", 88),
# ]
#
# columns = ["student", "subject", "marks"]
# df = spark.createDataFrame(data, columns)
# df.show()





#
# df.groupby("subject").agg(sum("marks").alias("sum")).show()
# df.groupby("student").agg(max("marks").alias("max")).show()
#
# df.groupby("student").agg(min("marks").alias("min")).show()
#
# df.groupby("subject").agg(avg("marks").alias("avg")).show()
#
# df.groupby("subject").agg(count("marks").alias("count")).show()


# df.groupby("Student").pivot("subject").agg(sum("marks").alias("sum")).show()
#
# df.createOrReplaceTempView("students")
#
# spark.sql("select * from students").show()
#
# spark.sql("select subject, max(marks) as total from students group by subject").show()
#
#
# spark.sql("select subject, min(marks) as min from students group by subject").show()
#
#
# spark.sql("select subject, floor(avg(marks)) as avgssl from students group by subject").show()
#
# spark.sql("select subject, count(marks) as counts from students group by subject").show()


# data = [
#     ("ravi", "pune", 32),
#     ("gautham", "delhi", 30),
#     ("mary", "noida", 35),
#     ("thomas", "delhi", 31),
#     ("shankar", "chennai", 30),
#     ("ravi", "chennai", 32),
# ]
#
# df = spark.createDataFrame(data, ["name", "city", "age"])
# df.show()
#
#
# window_spec = Window.partitionBy("city").orderBy(F.desc("age"))
#
# df1 = df.withColumn("rownumber", F.row_number().over(window_spec))
#
# df1.show()
#
# df2 = df.withColumn("dense", F.dense_rank().over(window_spec))
# df2.show()
#
# df3 = df.withColumn("rank", F.rank().over(window_spec))
#
# df3.show()
#
# df4=df.withColumn("RowNumner", F.row_number().over(window_spec))\
#       .withColumn("dense", F.dense_rank().over(window_spec))\
#     .withColumn("rank", F.rank().over(window_spec))
#
# df4.show()


customer_dim_data = [

(1,'manish','arwal','india','N','2022-09-15','2022-09-25'),
(2,'vikash','patna','india','Y','2023-08-12',None),
(3,'nikita','delhi','india','Y','2023-09-10',None),
(4,'rakesh','jaipur','india','Y','2023-06-10',None),
(5,'ayush','NY','USA','Y','2023-06-10',None),
(1,'manish','gurgaon','india','Y','2022-09-25',None),
]

customer_schema= ['id','name','city','country','active','effective_start_date','effective_end_date']

customer_dim_df = spark.createDataFrame(data= customer_dim_data,schema=customer_schema)

sales_data = [

(1,1,'manish','2023-01-16','gurgaon','india',380),
(77,1,'manish','2023-03-11','bangalore','india',300),
(12,3,'nikita','2023-09-20','delhi','india',127),
(54,4,'rakesh','2023-08-10','jaipur','india',321),
(65,5,'ayush','2023-09-07','mosco','russia',765),
(89,6,'rajat','2023-08-10','jaipur','india',321)
]

sales_schema = ['sales_id', 'customer_id','customer_name', 'sales_date', 'food_delivery_address','food_delivery_country', 'food_cost']

sales_df = spark.createDataFrame(data=sales_data,schema=sales_schema)


sales_df.show()

