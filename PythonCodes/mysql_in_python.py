print("MYSQL Using in Python")

from loguru import logger

import mysql.connector


connection=mysql.connector.connect(
host="localhost",
user="root",
password="Hadoop@123",
database="mkd"

)

# print(connection)

mycursor=connection.cursor()

mycursor.execute("select * from customer ")

for x in mycursor:
    print(x)