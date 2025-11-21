from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df_tab8 = spark.createDataFrame(
    [("India",), ("Pakistan",), ("SriLanka",)],
    ["teams"]
)

df_tab8.show()


from pyspark.sql.functions import *

df1=df_tab8.alias("a").join(df_tab8.alias("b"),col("a.teams")<col("b.teams") ,"inner"  )

df1.withColumn("Match", expr("concat(a.teams, ' VS ', b.teams)")   ).drop("teams","teams").show()


# USING SQL

df_tab8.createTempView("TAB")

spark.sql("select concat(a.teams ,'  VS ',b.teams) as TEAMS from TAB a join TAB b on a.teams<b.teams ").show()

