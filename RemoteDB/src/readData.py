import BTrees
from BTrees.OOBTree import OOBTree
import os
import re

class readData:
    def __init__(self,parsedData):
        self.ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
        self.parsedData = parsedData

    def read(self, TempTables = "Tables"):
        dataList = OOBTree()
        Lines = ""
        with open(self.ROOT_DIR + "/Database/"+ TempTables +"/" + self.parsedData["TableName"] + "/DataFile.txt", 'r') as readFile:
            Lines = readFile.readlines()
        if not Lines:
            with open(self.ROOT_DIR + "/Database/Tables/" + self.parsedData["TableName"] + "/DataFile.txt", 'r') as readFile:
                Lines = readFile.readlines()

        n = 0
        columns = []
        BtreeArray = {}

        for line in Lines:

                line = line.replace('\n', '').strip()
                if line:
                    if n == 0:
                        n = n + 1
                        columns = line.upper().split("||")
                        for col in columns:
                            BtreeArray[col] = OOBTree()
                    else:
                        data = line.split("||")
                        goAheadFlag = True

                        if (goAheadFlag):
                            for col in columns:
                                t = BtreeArray[col]
                                t.insert(n, data[columns.index(col)])
                                BtreeArray[col] = t
                            n = n + 1

        return BtreeArray



        '''
        for k in dataList:
            print("DP: ")
            print(dataList[k])
        '''

    def readWithWhereClause(self):
        dataList = OOBTree()
        with open(self.ROOT_DIR + "/Database/Tables/" + self.parsedData["TableName"] + "/DataFile.txt", 'r') as readFile:
            Lines = readFile.readlines()
            n = 0
            columns = []
            BtreeArray = {}

            for line in Lines:
                line = line.replace('\n', '').strip()
                if n == 0:
                    n = n + 1
                    columns = line.upper().split("||")

                    for col in columns:
                        BtreeArray[col] = OOBTree()
                else:
                    data = line.split("||")
                    goAheadFlag = True
                    #apply where clause
                    if len(self.parsedData["WhereClause"]) >0:

                        for clause in self.parsedData["WhereClause"]:
                            if isinstance(clause, list):
                                clause = tuple(clause)

                            if isinstance(clause, tuple):
                                value = clause[2].replace("'","").replace('"','')
                                if (clause[1] == "="):
                                    if data[columns.index(clause[0])].upper().strip() == value.upper().strip():
                                        goAheadFlag = True
                                    else:
                                        goAheadFlag = False

                    if (goAheadFlag):
                        for col in columns:
                            t = BtreeArray[col]
                            t.insert(n, data[columns.index(col)])
                            BtreeArray[col] = t
                        n = n + 1

        return BtreeArray

    def getCurrentTableAllCols(self, TableName):
        with open(self.ROOT_DIR + "/Database/Tables/" + TableName + "/DataFile.txt", 'r') as readFile:
            Lines = readFile.readlines()
            n = 0
            columns = []
            BtreeArray = {}

            for line in Lines:
                line = line.replace('\n', '').strip()
                if n == 0:
                    n = n + 1
                    columns = line.upper().split("||")
                    break
        return columns
