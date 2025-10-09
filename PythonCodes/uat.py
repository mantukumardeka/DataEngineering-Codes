

file="/Users/mantukumardeka/Desktop/data/salesorder.csv"


# Using WITH OPEN() 

with open("/Users/mantukumardeka/Desktop/data/salesorder.csv" ) as f:

 print(f.read())

 print(f.readline())