
# Creating DataFrame using pandas

import pandas

data={"car":["Ford","BMW","Maruti"],
      "passing":[1,2,3]
      
      }

mydataset=pandas.DataFrame(data)

print(data)

print(type(mydataset))
print(type(data))

