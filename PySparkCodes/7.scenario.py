from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("tab7").getOrCreate()

data = [
    (1, 100, 2010, 25, 5000),
    (2, 100, 2011, 16, 5000),
    (3, 100, 2012, 8, 5000),
    (4, 200, 2010, 10, 9000),
    (5, 200, 2011, 15, 9000),
    (6, 200, 2012, 20, 7000),
    (7, 300, 2010, 20, 7000),
    (8, 300, 2011, 18, 7000),
    (9, 300, 2012, 20, 7000)
]

columns = ["sale_id", "product_id", "year", "quantity", "price"]

df = spark.createDataFrame(data, columns)

df.show()


from pyspark.sql.window import Window
from pyspark.sql.functions import col, dense_rank

wins=Window.partitionBy("year").orderBy(col("quantity").desc())

final_df=df.withColumn("rnk", dense_rank().over(wins))

final_df.filter(col("rnk")==1).show()