import re
from src.ParsedData import *


class InsertQueryProcessor:
    def __init__(self, queryObj):
        self.queryObj = queryObj
        query = queryObj[0]
        self.ogQuery = query.strip()
        self.query = query.upper().strip()
        self.syntaxStatus = True
        self.response = ""

    def execute(self):
        self.parser()
        if self.syntaxStatus:
            self.checkRepeatedColumns()
        if self.syntaxStatus:
            self.parsedData.columnValue = (self.parsedData.parseInsertValues(self.parsedData.columnValue))
        if self.syntaxStatus:
            self.dataTypeChecker()
        if self.syntaxStatus:
            parsedObj = {
                'transactionId': self.queryObj[1],
                'queryType': self.parsedData.queryType,
                'TableName': self.parsedData.TableName,
                'columnName': self.parsedData.columnName,
                'columnValue': self.parsedData.columnValue,
                'WhereClause': self.parsedData.WhereClause,
                'UpdateSet': self.parsedData.UpdateSet
            }
            self.queryObj[2] = self.parsedData.getTableLocation(self.parsedData.TableName)
            self.queryObj[3] = True
            self.queryObj[4] = parsedObj

        else :
            self.queryObj[2] = self.parsedData.getTableLocation(self.parsedData.TableName)
            self.queryObj[3] = False
            self.response = self.response + ("Invalid syntax \n")
            self.queryObj[5] = self.response
        return self.queryObj

    def dataTypeChecker(self):
        columns = self.parsedData.columnName.split(",")
        values = self.parsedData.columnValue
            
        self.syntaxStatus = self.parsedData.checkDataType(self.parsedData.TableName, columns,values)
        if not self.syntaxStatus:
            self.response = self.response + "DataType mis-match \n"

    def checkRepeatedColumns(self):
        columns = self.parsedData.columnName.split(",")
        checkedColumn = []
        for col in columns:
            if (col.strip().upper() in checkedColumn):
                self.syntaxStatus = False
                self.response = self.response + "Repeated column name \n"
                break
            else:
                checkedColumn.append(col.strip().upper())

    def parser(self):
        if (bool(re.match("INSERT +INTO +([A-Za-z][A-Za-z0-9_-]*)(\s*)(?:\((.*)\))?(\s*)VALUES(\s*)(\((,)?(.*)\))*(\s*)([;]?)", self.query))):
            queryType = self.query.split(" ", 1)[0]
            temp = self.query.split(" ", 1)[1]
            temp = temp.split(" ", 1)[1]
            TableName = temp.split(" ", 1)[0]
            temp = temp.split(" ", 1)[1]
            columnName = temp.split(" VALUES", 1)[0]
            columnName = columnName[1:-1]
            if(self.ogQuery[-1] == ";"):
                temp = self.ogQuery[:-1]
            
            if (' values' in temp):
                temp = temp.split(" values")[1].strip()
            columnValue = temp[1:-1]

            self.parsedData = ParsedData(
                queryType, TableName, columnName, columnValue, "")
            self.syntaxChecker()
        else:
            self.syntaxStatus = False

    def syntaxChecker(self):
        # Check column Name Syntax
        csvColumnName = self.parsedData.columnName[1:-1].strip()
        csvColumnValue = self.parsedData.columnValue[1:-1].strip()
        if (bool(re.search(",", csvColumnName))):
            # more than one column
            tempCN = csvColumnName.split(",")
            self.syntaxStatus = bool(re.match("([A-Za-z][A-Za-z0-9_-]+[,]+[\s]*[A-Za-z][A-Za-z0-9_-]?)", csvColumnName))
            if (self.syntaxStatus and bool(re.search(",", csvColumnValue))):
                # more than one column values
                self.syntaxStatus = bool(re.match("([\s]*[']?[A-Za-z0-9\s.!@#$%^&_-]+[']?[\s]*)([,]+[\s]*[\s]*[']?[A-Za-z0-9\s.!@#$%^&_-]+[']?[\s]*)+", csvColumnValue))
                tempCV = csvColumnValue.split(",")
                if not (len(tempCN) == len(tempCV)):
                    self.syntaxStatus = False
                    self.response = self.response + ("Mismatch in Number of column and values \n")
            else :
                self.syntaxStatus = False
        else:
            # one column
            self.syntaxStatus = bool(re.match("([A-Za-z][A-Za-z0-9_-]+[\s]*)", csvColumnName))
            if self.syntaxStatus:
                multipleColumn = False
            if (self.syntaxStatus and not bool(re.search(",", csvColumnValue))):
                # one column
                self.syntaxStatus = bool(re.match("([\s]*[']?[A-Za-z0-9\s.!@#$%^&_-]+[']?[\s]*)", csvColumnValue))

            else:
                self.syntaxStatus = False



        # Check Column Value Syntax
