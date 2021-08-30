import json
import os
from prettytable import PrettyTable

class erdGenerator:
    def __init__(self):
        print("ERD Generation Started.")
    def execute(self):
        ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
        
        with open(ROOT_DIR + "/Database/Dictionaries/LocalDataDictionary.json", 'r') as readFile:
            self.data = json.load(readFile)
            self.Generate()
        print("ERD Generation Completed")

    def Generate(self):
        
        for o in self.data["TableDetails"]:
            ColumnDetails = o.get("ColumnDetails")
            header = (o.get("TableName"))
            colArray = []
            for col in ColumnDetails:
                temp = col.get("ColumnName") + ": " + col.get("DataType")
                if (col.get("PK") == "Y"):
                    temp = temp + " (PK)"
                if (col.get("FK") == "Y"):
                    temp = temp + " " + self.getRelation(o,col.get("ColumnName"))
                colArray.append(temp)
            myTable = PrettyTable()
            myTable.add_column(header, colArray)
            print(myTable)
           

    
    def getRelation(self,o,columnName):
        
            relation = ""
            constraints = o.get("Constraints")
            if(len (constraints) > 0):
                for c in constraints:
                    if (c.get("fkColumnName").upper() == columnName.upper().strip()):
                        #relation = o.get("TableName")
                        if (c.get("ConstraintName") == "FK_RELATION"):
                            relation = relation+"(FK:"+c.get("fkColumnName")+")->(PK:"+c.get("pkColumnName")+")"+c.get("pkTableName")
                        return"[" + (relation) + "]"

            else:
                relation = o.get("TableName")
                relation = relation + "(PK: "
                for col in o.get("ColumnDetails"):
                    if(col.get("PK") == "Y"):
                        relation = relation + col.get("ColumnName") + ")"
                print(relation)
                print()