# print("Reading from file and doing RDD Operation")
#
#
# from pyspark import  SparkContext
#
# sc=SparkContext("local","RDD Operation")
#
# rdd=sc.textFile("/Users/mantukumardeka/Desktop/data/st.txt")
# #rdd = sc.textFile("/Users/mantukumardeka/Desktop/data/scdata.txt")
#
# print(rdd.collect())
# #
# #
# # from pyspark.sql import SparkSession
# # from pyspark import  SparkContext
# # #
# # # spark=SparkSession.builder.appName("RDD").getOrCreate()
# # #
# # # sc=spark.sparkContext
# # #
# # # rdd1=sc.textFile("/Users/mantukumardeka/Desktop/data/scdata.txt")
# # #
# # # print(rdd1.collect())
# #
#
#
# print("Word Count program")
#
from pyspark import SparkContext
#
sc=SparkContext("local","Words Count")
#
#
# rdd=sc.textFile("/Users/mantukumardeka/Desktop/data/wordcounts.txt")
#
# print(rdd.collect())
#
# print("Splitting")
#
# split_rdd=rdd.flatMap(lambda x :x.split(" "))
#
# print(split_rdd.collect())
#
# print("Word COunt now")
#
# #count_rdd=split_rdd.map(lambda x:(x ,1 ))
#
# count_rdd = split_rdd.map(lambda x: (x, 1))
#
#
# print(count_rdd.count())
#
# # final_rdd=count_rdd.reduceByKey(_+_)
#
# final_rdd = count_rdd.reduceByKey(lambda a,b:a+b)
#
#
# print(final_rdd.collect())




# rdd2=sc.textFile("/Users/mantukumardeka/Desktop/data/*.txt")
# print(rdd2.collect())
#
# # Specify partitions during creation
# rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8], numSlices=4)
# print(f"Number of partitions: {rdd.getNumPartitions()}")  # 4
#
# # From file with partitions
# rdd = sc.textFile("/Users/mantukumardeka/Desktop/data/data.txt", minPartitions=8)
#
# # Repartition existing RDD
# rdd_repartitioned = rdd.repartition(10)
# print(f"After repartition: {rdd_repartitioned.getNumPartitions()}")  # 10
#
# # Coalesce (reduce partitions without shuffle)
# rdd_coalesced = rdd.coalesce(2)
# print(f"After coalesce: {rdd_coalesced.getNumPartitions()}")  # 2


from pyspark import SparkContext

sc = SparkContext("local", "RDD Partition Example")

data = [1, 2, 3, 4, 5, 6, 7, 8, 9]

# Create RDD with 3 partitions
rdd = sc.parallelize(data, numSlices=3)

print("Number of partitions:", rdd.getNumPartitions())



