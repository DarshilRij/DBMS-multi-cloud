import BTrees
from BTrees.OOBTree import OOBTree
import os
import re
import datetime
from RemoteDB.src.utility import *

class writeData:
    def __init__(self,parsedData= None):
        self.ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
        self.parsedData = parsedData
        self.lastExecutedQueryTableDetails = ""


    def writeAll(self, allColumns, dataList, tempTable = "Tables"):
        header = ""
        isDataPresent = False
        self.lastExecutedQueryTableDetails = self.lastExecutedQueryTableDetails + "Table " + self.parsedData["TableName"]

        for c in allColumns:
            header = header + c + "||"
            if (not (dataList[c] == None)):
                isDataPresent = True
        with open(self.ROOT_DIR + "/Database/"+ tempTable +"/" + self.parsedData["TableName"] + "/DataFile.txt", 'w') as writeFile:
            writeFile.write(header[:-2] + '\n')

        if (isDataPresent):
            dataRowIndexArray = list(dataList[allColumns[0]])
            i = 1
            for dataRow in dataRowIndexArray:
                data = ""
                for c in allColumns:
                    data = data + dataList[c][dataRow] + "||"
                with open(self.ROOT_DIR + "/Database/"+ tempTable +"/" + self.parsedData["TableName"] + "/DataFile.txt", 'a') as writeFile:
                    i = i + 1
                    writeFile.write(data[:-2] + '\n')

    def WriteTempData(self, TableList):
        if len(TableList)>0:
            for tableName in TableList:
                data = ""
                tableName = tableName.upper().strip()
                if utility().getTableLocation(tableName) == utility().getCurrentLocation():
                    tempPath = self.ROOT_DIR + "/Database/TempTables/"+ tableName.upper().strip() + "/DataFile.txt"
                    with open(tempPath, 'r') as readFile:
                        data = readFile.read()
                    with open(tempPath, 'w') as writeFile:
                        pass
                    permPath = self.ROOT_DIR + "/Database/Tables/"+ tableName.upper().strip() + "/DataFile.txt"
                    with open(permPath, 'w') as writeFile:
                        writeFile.write(data)

    def clearTempTable(self, TableList):
        if len(TableList)>0:
            for tableName in TableList:
                data = ""
                tableName = tableName.upper().strip()
                if utility().getTableLocation(tableName) == utility().getCurrentLocation():
                    tempPath = self.ROOT_DIR + "/Database/TempTables/"+ tableName.upper().strip() + "/DataFile.txt"
                    with open(tempPath, 'r') as readFile:
                        data = readFile.read()
                    with open(tempPath, 'w') as writeFile:
                        pass
