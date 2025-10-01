from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Leetcode_175").getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


# Data for person_175 table

person_data = [
    (1, "Wang", "Allen"),
    (2, "Alice", "Bob")
]

person_columns = ["PersonId", "FirstName", "LastName"]

# Create DataFrame
person_df = spark.createDataFrame(person_data, person_columns)

# Data for address_175 table
address_data = [
    (1, 2, "New York City", "New York")
]

address_columns = ["AddressId", "PersonId", "City", "State"]

# Create DataFrame
address_df = spark.createDataFrame(address_data, address_columns)
person_df.show()
address_df.show()


print("Left Joining")

#Write a solution to report the first name, last name, city, and state of each person in the Person table.
# If the address of a personId is not present in the Address table, report null instead.

person_df.join(address_df, "PersonId", "left").show()


# Left Joining
# +--------+---------+--------+---------+-------------+--------+
# |PersonId|FirstName|LastName|AddressId|         City|   State|
# +--------+---------+--------+---------+-------------+--------+
# |       1|     Wang|   Allen|     NULL|         NULL|    NULL|
# |       2|    Alice|     Bob|        1|New York City|New York|
# +--------+---------+--------+---------+-------------+--------+


print("Using SQL" )

person_df.createOrReplaceTempView("person")
address_df.createOrReplaceTempView("address")

spark.sql("select * from person").show()
spark.sql("select * from address").show()


print("Left Joining")
spark.sql("select p.firstname,p.lastname,a.state from person p left join address a on p.personid=a.personid").show()

#
# Left Joining
# +---------+--------+--------+
# |firstname|lastname|   state|
# +---------+--------+--------+
# |     Wang|   Allen|    NULL|
# |    Alice|     Bob|New York|
# +---------+--------+--------+
