from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

conf = SparkConf().setMaster("local[*]").setAppName("MaskingExample")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
spark = SparkSession.builder.getOrCreate()



#df=spark.read.json("file:///Users/mantukumardeka/Desktop/DataEngineering/test.json")

df = spark.read.format("json").option("multiline", "true").load("file:///Users/mantukumardeka/Desktop/DataEngineering/test.json")
df.printSchema()

