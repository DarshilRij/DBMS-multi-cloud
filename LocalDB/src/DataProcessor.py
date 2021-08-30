from src.ParsedData import *
from src.readData import *
from src.writeData import *
from src.GeneralLogger import *
from src.EventLogger import *
from src.utility import *

import os
import json
from prettytable import PrettyTable

class DataProcessor:
    def __init__(self,queryObj, parsedData):
        self.queryObj =queryObj
        self.parsedData = parsedData
        self.sematicsCheckerFlag = True
        self.AffectedRows = 0
        self.response = ""
        self.ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
        self.EL = EventLogger()

    def execute(self):
        # check table lock
        tableName = self.parsedData["TableName"]
        if utility().isTableLocked(tableName, self.queryObj[1]):
            self.queryObj[3] = False
            self.queryObj[5] = self.response + "Table %s locked\n"%(tableName)
            return self.queryObj

        if not ( self.parsedData["queryType"].upper().strip() == "SELECT"):
            utility().lockTable(tableName, self.queryObj[1])
            self.EL.logMessage("Lock acquired - Table %s"%(tableName))
        try:
            #check GDD from table location
            self.sematicsChecker()
            if(self.sematicsCheckerFlag):
                self.response = self.process()
            else:
                self.response = self.response + ("Semantic Error \n")
                self.queryObj[3] = False
        except Exception as e:
            self.queryObj[3] = False
            self.EL.logMessage("Exception occured : "  + str(e))
        finally:
            self.queryObj[5] = self.response
            if not (self.parsedData["transactionId"]):
                utility().releaseLock(self.queryObj[1])
                self.EL.logMessage("Lock released - Table %s"%(tableName))
        return self.queryObj

    def sematicsChecker(self):
        columnCheckFlag = True
        whereClauseCheckFlag = True
        self.location = utility().getTableLocation(self.parsedData["TableName"])
        self.columnMetadata= []
        with open(self.ROOT_DIR + "/Database/Dictionaries/LocalDataDictionary.json", 'r') as readFile:
            data = json.load(readFile)
            for d in data["TableDetails"]:
                if (d.get("TableName").upper() == self.parsedData["TableName"].upper()):
                    self.columnMetadata = d.get("ColumnDetails")

        if (len(self.location) <1):
            self.response = self.response +  ("Table Not Found\n")
            self.queryObj[3] = False
        if (len(self.parsedData["columnName"]) > 0):
            columnCheckFlag = self.verifyColumns()

        if (len(self.parsedData["WhereClause"]) > 0):
            whereClauseCheckFlag = self.verifyWhereClause()

        if(self.location == "" or not columnCheckFlag or not whereClauseCheckFlag):
            self.sematicsCheckerFlag = False

    def verifyColumns(self):
        columnVerificationFlag = True
        #print("DP: "+ self.parsedData["columnName"].strip())
        if(self.parsedData["columnName"].strip() == "*"):
            tempcol = ""
            for col in self.columnMetadata:
                tempcol = tempcol  + col.get("ColumnName") + ","

            self.parsedData["columnName"] = tempcol[:-1]
        else:
            colummns = self.parsedData["columnName"].split(",")
            for col in colummns:
                found = False
                #print("DP: " + col.strip().upper())
                for metacol in self.columnMetadata:
                    #print("DP: " + metacol.get("ColumnName").upper().strip())
                    if (col.strip().upper() == metacol.get("ColumnName").upper()):
                        found = True
                if (not found):
                    columnVerificationFlag = False
                    self.response = self.response + ("Column Not Found \n")
                    self.queryObj[3] = False
                    break
        return columnVerificationFlag

    def verifyWhereClause(self):
        whereClauseVerificationFlag = True
        for clause in self.parsedData["WhereClause"]:
            #print("DP:")
            #print((clause))
            if isinstance(clause, str):
                continue
            found = False
            for metacol in self.columnMetadata:
                if (clause[0].upper() == metacol.get("ColumnName").upper()):
                    found = True

            if (not found):
                whereClauseVerificationFlag = False
                self.response = self.response + ("Where Clause Column Not Found \n")
                self.queryObj[3] = False
                break
        return whereClauseVerificationFlag

    def process(self):
        self.AffectedRows = 0
        queryType = self.parsedData["queryType"].upper()
        response = ""
        if queryType == "INSERT":
            self.read()
            self.insert()
            response = self.response + str(self.AffectedRows) + " Row were affected."
        elif queryType == "UPDATE":
            self.read()
            self.update()
            response = self.response + str(self.AffectedRows) + " Row were affected."
        elif queryType == "SELECT":
            self.readWithWhereClause()
            response = self.select()
        elif queryType == "DELETE":
            self.read()
            self.delete()
            response = str(self.AffectedRows) + " Rows were affected."
        return response

    def read(self):
        ReadData = readData(self.parsedData)
        tablePath = utility().tablePath(self.parsedData["TableName"], self.queryObj[1])

            
        self.dataList = ReadData.read(tablePath)
        self.CurrentTableAllCols = ReadData.getCurrentTableAllCols(self.parsedData["TableName"])

    def readWithWhereClause(self):
        self.dataList = readData(self.parsedData).readWithWhereClause()

    def write(self):
        WRTData = writeData(self.parsedData)
        WRTData.writeAll(self.CurrentTableAllCols, self.dataList, "TempTables")
        
        GeneralLogger().writeDBState(self.parsedData["queryType"], self.parsedData["TableName"],str(len(list(self.dataList[self.CurrentTableAllCols[0]].values()))))

    def update(self):
        columnArray = self.CurrentTableAllCols
        clauseIndexArray = []
        updatedAT= ""
        for clause in self.parsedData["WhereClause"]:
            indexArray = []
            if isinstance(clause, tuple):
                updatedAT = updatedAT + clause[0] + clause[1] + clause[2]
                BTreeByValue = ((self.dataList[clause[0].upper().strip()].byValue(None)))
                for val in BTreeByValue:
                    if val[0] == clause[2][1:-1]:
                        indexArray.append(val[1])
                clauseIndexArray.append(indexArray)

        AffectedRowsArray = []
        avoidIndexArray = []
        setAT = ""
        for clause in self.parsedData["UpdateSet"]:
            if isinstance(clause,tuple):
                setAT  = setAT + " update " + clause[0] + " to " + clause[2]
                BtreeVal = self.dataList[clause[0].upper().strip()]

                for indexArray in clauseIndexArray:
                    for index in indexArray:
                        if not (index in avoidIndexArray):
                            if (self.PKcheck(clause[0], clause[2][1:-1], BtreeVal)):
                                (BtreeVal[index]) = clause[2][1:-1]
                                if not (index in AffectedRowsArray):
                                    AffectedRowsArray.append(index)
                            else:
                                if not (index in avoidIndexArray):
                                    avoidIndexArray.append(index)

                        #print (list(BtreeVal.values()))
                self.dataList[clause[0].upper().strip()] = BtreeVal

        self.write()
        self.EL.logMessage("Request to " + setAT + " where " + updatedAT)
        self.AffectedRows = len(AffectedRowsArray)

    def insert(self):
        columnArray = self.CurrentTableAllCols
        QureyColumns = self.parsedData["columnName"].replace(" ","").split(",")
        QureyValues = self.parsedData["columnValue"]
        saveFlag = False
        valueAT = ""
        for col in columnArray:
            col = col.strip().upper()
            val = ""
            ColumnBtree = self.dataList[col]
            NewIndex = (len(list(ColumnBtree))) + 1
            if (col in QureyColumns):
                val = (QureyValues[QureyColumns.index(col)][1:-1])
            if self.PKcheck(col, val, ColumnBtree):
                saveFlag = True
                ColumnBtree.insert(NewIndex, val)
                if col in QureyColumns:
                    valueAT = valueAT + (QureyValues[QureyColumns.index(col)]) + ", "
            else:
                break

        if saveFlag:
            self.write()
            self.EL.logMessage("Inserted Values " + valueAT+ " in table " + self.parsedData["TableName"])
            self.AffectedRows = self.AffectedRows +1

    def PKcheck(self, colName, value, colBtree):
        status =True
        for detail in self.columnMetadata:
            if colName == detail.get("ColumnName").strip().upper():
                if detail.get("PK").strip().upper() == "Y":
                    if value in colBtree.values():
                        self.response = self.response + "Duplicate Primary Key \n"
                        self.queryObj[3] = False
                        status = False
                        break
        return status

    def select(self):
        colsArray = self.parsedData["columnName"].upper().strip().split(",")
        showList = []
        for c in colsArray:
            colDataList = []
            c = c.upper().strip()
            for k in self.dataList[c]:
                colDataList.append(self.dataList[c][k])
            showList.append(colDataList)
        return self.getPrettyTable(colsArray, showList)

    def delete(self):
        whereAT= ""
        if len (self.parsedData["WhereClause"])>0:
            for clause in self.parsedData["WhereClause"]:
                if isinstance(clause, list):
                    clause =tuple(clause)
                if isinstance(clause, tuple):
                    whereAT = whereAT + str(clause[0]) + str(clause[1])+ str(clause[2])
                    whereCol = clause[0].strip().upper()
                    if(isinstance(clause[2],str)):
                        value = clause[2][1:-1]
                    else:
                        value = clause[2]
                    if (clause[1] == "="):
                        while (len(list(self.dataList[whereCol]))> 0 and ( (value) in list(self.dataList[whereCol].values())) ):
                            index = None
                            for datatup in list(self.dataList[whereCol].iteritems()):
                                if datatup[1] == value:
                                    index = datatup[0]
                                    break

                            for i in (list(self.dataList[whereCol])):
                                if(self.dataList[whereCol][index] == self.dataList[whereCol][i]):
                                    self.AffectedRows = self.AffectedRows + 1
                                    for c in self.CurrentTableAllCols:
                                        del self.dataList[c][i]
                                    break

        else:
            for c in self.CurrentTableAllCols:
                self.AffectedRows = len(list(self.dataList[c]))
                self.dataList[c] = None
        self.write()
        self.EL.logMessage("Deleted "+ str(self.AffectedRows) + " row(s) where " +whereAT )
        #for c in self.CurrentTableAllCols:
        #    print(len(list(self.dataList[c].values())))

    def getPrettyTable(self,colsArray, showList):
        myTable = PrettyTable()
        for c in colsArray:
            myTable.add_column(c, showList[colsArray.index(c)])
        return str(myTable)

        '''
        myTable = PrettyTable(colsArray)
        for c in colsArray:
            myTable.add_row(["Leanord", "X", "B", "91.2 %"])
        '''