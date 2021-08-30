import json
import os
import calendar
import time

class sqlDumpGenerator:
    def __init__(self):
        print("SQL Dump Generation Started.")
    def execute(self):
        self.ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
        
        with open(self.ROOT_DIR + "/Database/Dictionaries/LocalDataDictionary.json", 'r') as readFile:
            self.data = json.load(readFile)
            self.Generate()
        print("SQL Dump file is present at: "+ self.ROOT_DIR + "/SQL_Dump")
        print("Name of the file is " + self.SqlDumpFileName)
        print("SQL Dump Generation Completed")
        
    def Generate(self):
        sqlDump = []
        for o in self.data["TableDetails"]:
            print()
            query = ""
            query = "CREATE TABLE "+ o.get("TableName") + "( "
            columnPartofQuery = "" 
            #create the column part
            for col in o.get("ColumnDetails"):
                columnPartofQuery = columnPartofQuery + col.get("ColumnName") + " " + col.get("DataType") + ", "
            

            PKConstraintPart = ""
            #create the PK contraint part
            for col in o.get("ColumnDetails"):
                if(col.get("PK") == "Y"):
                    PKConstraintPart = PKConstraintPart + "PRIMARY KEY ("+ col.get("ColumnName") +"), "
            
            #create FK constraint part 
            FKConstraintPart = ""
            constraints = o.get("Constraints")
            if(len (constraints) > 0):
                for c in constraints:
                    FKConstraintPart = FKConstraintPart + "FOREIGN KEY ("  + c.get("fkColumnName")+") REFERENCES "+ c.get("pkTableName") + "(" + c.get("pkColumnName")+"), "


            query = query + columnPartofQuery + PKConstraintPart + FKConstraintPart 
            if(query.strip()[-1]):
                query = query.strip()[:-1]
            query = query + ");"

            sqlDump.append(query)
        gmt = time.gmtime()
        ts = calendar.timegm(gmt)
        self.SqlDumpFileName = 'sqlDump'+ str(ts)+'.sql'
        for sql in sqlDump:
            with open(self.ROOT_DIR + "/SQL_Dump/" + self.SqlDumpFileName, 'a') as f:
                f.write(sql + '\n')