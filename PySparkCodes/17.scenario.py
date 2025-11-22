from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("emp_data").getOrCreate()

data = [
    (1, "Tim", 24, "Kerala", "India"),
    (2, "Asman", 26, "Kerala", "India")
]

df = spark.createDataFrame(
    data,
    ["emp_id", "name", "age", "state", "country"]
)

df.show()
data1 = [
    (1, "Tim", 24, "Comcity"),
    (2, "Asman", 26, "bimcity")
]

df1 = spark.createDataFrame(
    data1,
    ["emp_id", "name", "age", "address"]
)

df1.show()

joindf=df1.join(df,["emp_id"],"inner").select(df.emp_id,df.name,df.age,df.state,df.country,df1.address)

joindf.show()

# USING MYSQL


df.createTempView("tab17_1")

df1.createTempView("tab17_2")
spark.sql("select  a.emp_id,a.name,a.age,a.state,a.country,b.address from tab17_1 a join tab17_2 b on a.emp_id=b.emp_id").show()