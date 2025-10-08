
#Interview Questions-
#
# input_dict={"a": 100, "b": 200, "c": 300}
#
# print(input_dict)
#
# print(input_dict["a"])
#
# print(input_dict.values())
#
# print(input_dict.items())
# print(input_dict.keys())
#
# res=sum(input_dict.values())
# print(res)


#
# lst = ['DIRECTED EDGE FRIEND', 'UNDIRECTED EDGE FOLLOW']
#
# for item in lst:
#     if 'DIRECTED' in item:
#         print(item)
#     elif 'UNDIRECTED' in item:
#         print(item)

# import datetime
# print(datetime.datetime.now())
#
# from datetime import datetime
#
# ### Get current datetime
# now = datetime.now()

### Print date, time, and day
# print("Current Date:", now.date())
# print("Current Time:", now.strftime("%H:%M:%S"))
# print("Day of Week:", now.strftime("%A"))
#
#
#
# #
# # sample = [{"id": 1, "Name" : "ABC"}, {"id" : 2, "Name": "xyZ"}, {"id": 3, "Name" : "ABC"}, {"id": 4, "Name" : "PQR"}, {"id": 5, "Name" : "XYZ"}]
#
#
# a = "aabccccddeeeee"
# str1 = ''
# str2 = ''
# distinct = set(a)
# for i in distinct:
#     count=0
#     for j in a:
#         if i==j:
#             count += 1
#         else:
#             continue
#     str1 = str1 + i + str(count)
# print(str1)
#
# arr = [12, 45, 7, 89, 34, 2]
#
# smallest = largest = arr[0]
#
# for num in arr:
#     if num < smallest:
#         smallest = num
#     elif num > largest:
#         largest = num
#
# print("Smallest:", smallest)
# print("Largest:", largest)
#
#
#
# print("Disable Autocompletion")
#

# data=[1,2,3,4,5,6,7,8,9]
#
# for x in data:
#     print(x*2)
#
#
# data1=[x*2 for x in data]
# print(data1)


from pyspark import SparkContext

sc = SparkContext("local", "RDD Partition Example")
#
# data = [1, 2, 3, 4, 5, 6, 7, 8, 9]
#
# # Create RDD with 3 partitions
# rdd = sc.parallelize(data, numSlices=5)
#
# print("Number of partitions:", rdd.getNumPartitions())


# Specify partitions during creation
rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8], numSlices=4)
print(f"Number of partitions: {rdd.getNumPartitions()}")  # 4

# From file with partitions
rdd = sc.textFile("/Users/mantukumardeka/Desktop/data/scdata.txt", minPartitions=8)

# Repartition existing RDD
rdd_repartitioned = rdd.repartition(10)
print(f"After repartition: {rdd_repartitioned.getNumPartitions()}")  # 10

# Coalesce (reduce partitions without shuffle)
rdd_coalesced = rdd.coalesce(2)
print(f"After coalesce: {rdd_coalesced.getNumPartitions()}")  # 2

rdd_repartitioned.getNumPartitions()
