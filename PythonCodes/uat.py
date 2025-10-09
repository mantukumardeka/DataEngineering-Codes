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

# for x in mycursor:
#     print(x)

result=mycursor.fetchall()

logger.info(f"Here is the output: {result}")


## Inserting values in table:

mycursor.execute("insert into customer (customer_id,customer_name) values (13,'Dipalim')")

## You have to run commit to reflect the data in the mysql
connection.commit()

connection.close()
mycursor.close()