from ftplib import print_line

print("How to create RDD")

# from pyspark import SparkContext
#
# sc=SparkContext("local","RDD Creation")
#
# rdd1=sc.parallelize([1,2,3,4,5,6,7,8])
#
# print(type(rdd1))
# print(rdd1.collect())


print("OR - Here another way by creating SparkSession")

from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("RDD").getOrCreate()

sc=spark.sparkContext
#
# rdd1=sc.textFile("/Users/mantukumardeka/Desktop/data/scdata.txt")
#
# print(rdd1.collect())
#
# print("Here is the LIST")
# data=[1,2,3,4,5,6,7,8,9]
#
# data_rdd=sc.parallelize(data)
#
# print(data_rdd.collect())
#
# print("Printing Each line (rdd2.foreach(println)-in scala")
#
# for x in data_rdd.collect():
#     print(x)
#
#
# print("Filtering list using LAMBDA greater than 2")
#
# filter_data=data_rdd.filter(lambda x:x>2)
#
# print(filter_data.collect())
#
# print("Multiplying  it with 2 using lamba")
#
#
# multiply_rdd=data_rdd.map(lambda x:x*20)
# print(multiply_rdd.collect())

# ls = ["Dutt","Mehta", "Deka"]
#
# rdd_name=sc.parallelize(ls)
#
# print(rdd_name.collect())
#
#
# print("Adding some character in Name")
#
# rename_rdd=rdd_name.map(lambda x:x +(  " : Indian Title"))
#
# print(rename_rdd.collect())
#
# print("Example of Filter")
# tt = ["Dutt", "Mehta", "Deka", "Sarma", " Boro"]
#
# tt_rdd=sc.parallelize(tt)
#
# print(tt_rdd.collect())
#
# print("Doing filter operation using filter")
#
# filter_rdd=tt_rdd.filter(lambda x: "a" in x)
#
# print(filter_rdd.collect())


# print("Example of FlatMap")
#
# ls =["A-B", "C-D", "E-F", "G-H"]
#
# rdd_ls=sc.parallelize(ls)
#
# print(rdd_ls.collect())
#
# print("Doing FlatMap Operation")
#
# flat_rdd=rdd_ls.flatMap(lambda x: x.split("-") )
#
# print(flat_rdd.collect())
#
#
# for x in flat_rdd.collect():
#     print(x)




print("RDD Operation")

name= [ "Amazon-Jeff-America",
					"Microsoft-BillGates-America",
					"TCS-TATA-india",
					"Reliance-Ambani-india"]

# Requirements-
#
# 1 ---- Filter elements contains india
# 2 ---- Flatten with delimiter  " - "
# 3 ---- replace "india" with "local"
# 4 ---- Convert all the string to lower case


# name_rdd=sc.parallelize(name)
#
# print(name_rdd.collect())
#
# print("1 ---- Filter elements contains india")
#
# filter_rdd=name_rdd.filter(lambda x: "india" in x )
#
# print(filter_rdd.collect())
#
# print("2 ---- Flatten with delimiter  '-' ")
#
# flater_rdd=filter_rdd.flatMap(lambda x : x.split("-")  )
#
# print(flater_rdd.collect())
#
#
# print('# 3 ---- replace "india" with "local"')
#
# replace_rdd=flater_rdd.map(lambda x: x.replace("india","local")  )
#
# print(replace_rdd.collect())
#
#
# print("# 4 ---- Convert all the string to lower case")
#
# lower_rdd=replace_rdd.map(lambda x: x.lower())
#
# print(lower_rdd.collect())



### Task 10

print("- Separate state and city data")

data=[
    "State->TamilNadu~City->Chennai",
    "State->Karnataka~City->Bangalore",
    "State->Telangana~City->Hyderabad"
]

## - Separate state and city data

#  Final State data
#
# TamilNadu
# Karnataka
# Telangana
#
# Finale city data
#
# Chennai
# Bangalore
# Hyderabad


#
# data_rdd=sc.parallelize(data)
#
# print(data_rdd.collect())
#
# print("Split part")
#
# split_rdd=data_rdd.flatMap(lambda x: x.split("~"))
#
# print(split_rdd.collect())
#
# for x in split_rdd.collect():
#     print(x)
#
# city_RDD=split_rdd.filter(lambda x: "City" in x )
# state_RDD=split_rdd.filter(lambda  x: "State" in x)
#
# for x in city_RDD.collect():
#     print(x)
#
# for x in split_rdd.collect():
#     print(x)
#
#
#
# print(" ============State Data===========")
#
# finale_st=state_RDD.map(lambda x:x.replace("State->", ""))
#
# for st in finale_st.collect():
#     print(st)
# print("============City Data==============")
#
# final_ct=city_RDD.map(lambda x:x.replace("City->", "" ))
# for ct in final_ct.collect():
#     print(ct)


ls = [
    "BigData-Spark-Hive",
    "Spark-Hadoop-Hive",
    "Sqoop-Hive-Spark",
    "Sqoop-BD-Hive"
]


ls_rdd=sc.parallelize(ls)

print(ls_rdd.collect())

fl_rdd=ls_rdd.flatMap(lambda x: x.split("-"))
print(fl_rdd.collect())

dd_rrd=fl_rdd.distinct()
print(dd_rrd.collect())

new_rdd=dd_rrd.map(lambda x: x + (  " -: MKD MIXture is trainer"))

print(new_rdd.collect())

for x in new_rdd.collect():
    print(x)












