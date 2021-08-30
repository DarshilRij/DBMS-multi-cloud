'''
from BTrees.OOBTree  import OOBTree



t = OOBTree()
a = {"Name" : "Hamza"}
t.insert(1,"a")
t[1] = "c"
t.insert(1,"b")
#update({1: {"Name" : "Hamza"}, 2: "green", 3: "blue", 4: "spades"})
print("Usa" in t.values())

for pair in t.iteritems():  # new in ZODB 3.3
    print (pair)
'''

'''
import logging

from src.InsertQueryProcessor import *
from src.SelectQueryProcessor import *
from src.DeleteQueryProcessor import DeleteQueryProcessor

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(name)s: %(message)s")

#query = input("Enter the query: ")
query = "SELECT *  FROM Customers where sd=a AND a=a;"
queryType = query.split(" ", 1)[0].upper()

if queryType == "INSERT":
    insertQuery = InsertQueryProcessor(query)
    insertQuery.execute()
elif queryType == "UPDATE":
    print(queryType)
elif queryType == "SELECT":
    selectQuery = SelectQueryProcessor(query)
    selectQuery.execute()
elif queryType == "DELETE":
    deleteQuery = DeleteQueryProcessor(query)
    deleteQuery.execute()
'''