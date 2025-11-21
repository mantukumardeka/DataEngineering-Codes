from matplotlib.patheffects import withStroke
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = spark.createDataFrame(
    [
        (1, "Jhon", 4000),
        (2, "Tim David", 12000),
        (3, "Json Bhrendroff", 7000),
        (4, "Jordon", 8000),
        (5, "Green", 14000),
        (6, "Brewis", 6000)
    ],
    ["emp_id", "emp_name", "salary"]
)

df.show()

from pyspark.sql.functions import *


gradedf=df.withColumn("GRADE", expr(  "case when salary >10000 then 'GRADE A' when salary between 10000 and 5000 then 'GRADE B' else 'GRADE C' end"  )  )

gradedf.show()