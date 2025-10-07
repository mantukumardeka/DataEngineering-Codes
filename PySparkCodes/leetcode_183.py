# Input:
# Customers table:
# +----+-------+
# | id | name  |
# +----+-------+
# | 1  | Joe   |
# | 2  | Henry |
# | 3  | Sam   |
# | 4  | Max   |
# +----+-------+
# Orders table:
# +----+------------+
# | id | customerId |
# +----+------------+
# | 1  | 3          |
# | 2  | 1          |
# +----+------------+
# Output:
# +-----------+
# | Customers |
# +-----------+
# | Henry     |
# | Max       |
# +-----------+
#
#
# Write a solution to find all customers who never order anything.


from pyspark.sql import SparkSession


spark=SparkSession.builder.appName("Leetcode_183").getOrCreate()

data=[ (1, "Joe"),
    ( 2, "Henry"),
    (3, "Sam"),
    (4, "Max")]

schema1 = ["id","name"]

customer_df=spark.createDataFrame(data,schema1)

customer_df.show()


data1=[(1,3),(2,1)]

schema2=["id","customerid"]

order_df=spark.createDataFrame(data1,schema2)

order_df.show()

print(" Write a solution to find all customers who never order anything.")

new_df=customer_df.join(order_df,customer_df.id==order_df.customerid,"left")\
    .select(customer_df.name).filter(order_df.customerid.isNull())

new_df.show()


print(" =========Here you will Get more about Logical and Physical Plan=========")
new_df.explain(True)


print("USING SQL")

customer_df.createOrReplaceTempView("customer")
order_df.createOrReplaceTempView("order")

spark.sql("select c.name from customer c left join order o on c.id=o.Customerid where o.customerid is null;")
