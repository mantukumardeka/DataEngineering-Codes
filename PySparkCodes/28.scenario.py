from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("tab28").getOrCreate()

data = [
    ("A", "AA"),
    ("B", "BB"),
    ("C", "CC"),
    ("AA", "AAA"),
    ("BB", "BBB"),
    ("CC", "CCC")
]

columns = ["child", "parent"]

df = spark.createDataFrame(data, columns)

df.show()


from pyspark.sql.functions import *

joindf = df.alias("a").join(df.alias("b"), col("a.child") == col("b.parent")).select(
    col("a.child").alias("child_a"),
    col("a.parent").alias("parent_a"),
    col("b.child").alias("child_b"),
    col("b.parent").alias("parent_b")
)
joindf.show()

findf = joindf.withColumnRenamed("child_a", "parent").withColumnRenamed("parent_a", "grandparent").withColumnRenamed(
    "child_b", "child").drop("parent_b").select("child", "parent", "grandparent")

findf.show()

# another way

df2 = df.withColumnRenamed("child", "child1").withColumnRenamed("parent", "parent1")
df2.show()

secondjoindf = df.join(df2, col("parent") == col("child1"), "inner")
secondjoindf.show()

finaldf = secondjoindf.withColumnRenamed("parent1", "grandparent").drop("child1")
finaldf.show()


print("CHATGPT WAY")


# Self Join: parent of child = child of another row
df_gp = (
    df.alias("c")
      .join(df.alias("p"), col("c.parent") == col("p.child"))
      .select(
          col("c.child").alias("child"),
          col("c.parent").alias("parent"),
          col("p.parent").alias("grandparent")
      )
)

df_gp.show()