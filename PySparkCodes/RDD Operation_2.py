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
# #
# # spark=SparkSession.builder.appName("RDD").getOrCreate()
# #
# # sc=spark.sparkContext
# #
# # rdd1=sc.textFile("/Users/mantukumardeka/Desktop/data/scdata.txt")
# #
# # print(rdd1.collect())
#


print("Word Count program")

from pyspark import SparkContext

sc=SparkContext("local","Words Count")


rdd=sc.textFile("/Users/mantukumardeka/Desktop/data/wordcounts.txt")

print(rdd.collect())

print("Splitting")

split_rdd=rdd.flatMap(lambda x :x.split(" "))

print(split_rdd.collect())

print("Word COunt now")

#count_rdd=split_rdd.map(lambda x:(x ,1 ))

count_rdd = split_rdd.map(lambda x: (x, 1))


print(count_rdd.count())

# final_rdd=count_rdd.reduceByKey(_+_)

final_rdd = count_rdd.reduceByKey(lambda a,b:a+b)


print(final_rdd.collect())
