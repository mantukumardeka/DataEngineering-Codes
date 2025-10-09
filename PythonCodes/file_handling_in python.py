

file="/Users/mantukumardeka/Desktop/data/salesorder.csv"

f=open(file,"rt")

# print(f.read())

print("Reading 1st 5 character in Python")

#print(f.read(5))

print("Reading first line in Python")

print(f.readline())

print("Reading 5 lines ")

print(f.readlines()[:5])

print("Reading all lines")

print(f.readlines())


# | Feature        | `readline()`              | `readlines()`             |
# | -------------- | ------------------------- | ------------------------- |
# | Reads          | One line at a time        | All lines at once         |
# | Return type    | String                    | List of strings           |
# | Memory use     | Low                       | High                      |
# | Best for       | Large files / streaming   | Small files               |
# | Moves pointer? | Yes (next line each time) | Reads entire file at once |



# Using WITH OPEN() 

with open("/Users/mantukumardeka/Desktop/data/salesorder.csv" ) as f:

 print(f.read())

 print(f.readline())

# CREATE A FILE IN DESKTOP AND READ IT ("r+" -- READ, WRITE)
 with open ("/Users/mantukumardeka/Desktop/delete.txt","r+")  as f:
    f.write("Hello words- Hello Guyes") 
    f.seek(0) # pointed to the beginneing

    print(f.read())  




