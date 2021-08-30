import re
from RemoteDB.src.ParsedData import *



class SelectQueryProcessor:
    def __init__(self, queryObj):
        self.queryObj = queryObj
        query = queryObj[0]
        self.ogQuery = query.strip()
        self.query = query.upper().strip()
        self.syntaxStatus = True
        self.response = ""
        self.parsedData = None

    def execute(self):
        self.parser()
        if self.syntaxStatus:
            #print("SQP: ")
            #print(self.parsedSelectData.queryType)
            #print(self.parsedSelectData.TableName)
            #print(self.parsedSelectData.columnName)
            #print(self.parsedSelectData.columnValue)
            #print(self.parsedSelectData.WhereClause)
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

    def parser(self):
        queryType = self.query.split(" ", 1)[0]
        temp = self.query.split(" ", 1)[1]
        WhereClause = ""
        if (bool(re.search("WHERE", self.query))):
            if (bool(re.match("SELECT +(?:(.*))?\s* +FROM ([A-Za-z][A-Za-z0-9._()]+) WHERE +(?:(.*))?", self.query))):
                WhereClause = temp.split(" WHERE", 1)[1].strip()
                if (bool(re.search(";", WhereClause))):
                    WhereClause = WhereClause.replace(';', '').strip()
                if (len(WhereClause) == 0):
                    self.syntaxStatus = False
                    self.response = self.response + ("Missing Where Clause \n")
            else:
                self.syntaxStatus = False
        elif (bool(re.match("SELECT +(?:(.*))?\s* +FROM +([A-Za-z][A-Za-z0-9._()]+)", self.query))):
            self.syntaxStatus = True
        else:
            self.syntaxStatus = False

        if(self.syntaxStatus):
            columnName = temp.split(" FROM", 1)[0].strip()
            temp = temp.split(" FROM", 1)[1]
            if (bool(re.search("WHERE", temp))):
                TableName = temp.split("WHERE", 1)[0].strip()
            else:
                TableName = temp.split(";", 1)[0].strip()
            self.parsedData = ParsedData(
                queryType, TableName, columnName, "", WhereClause)
            self.syntaxChecker()

    def syntaxChecker(self):
        columnName = self.parsedData.columnName
        if(bool(re.search(",", columnName)) or bool(re.search(" ", columnName))):
            '''
            if(bool(re.search(",", columnName))):
                print("Multi col")
            el
            '''
            if not (bool(re.search(",", columnName))):
                self.response = self.response + ("Missing comma (,) between column name \n")
                self.syntaxStatus = False

        WhereClause = self.parsedData.WhereClause
        if (WhereClause == ""):
            self.parsedData.WhereClause = tuple()
        else:
            self.parsedData.WhereClause = self.parsedData.parseWhereClause(WhereClause)
            if(len(self.parsedData.WhereClause)<1):
                self.syntaxStatus = False
                self.response = self.response + ("Incorrect where clause \n")

        '''
        temp = []
        if(bool(re.search("AND", WhereClause)) or  bool(re.search("OR", WhereClause))):
            if(bool(re.search("AND", WhereClause))):
                temp = WhereClause.split("AND")
            if(bool(re.search("OR", WhereClause))):
                temp = WhereClause.split("OR")
            for part in temp:
                part = part.strip()
                if not part:
                    print("Incorrect Where Clause")
                    self.syntaxStatus = False
                else:
                    self.checkWhereClause(part)


    def checkWhereClause(self, WhereClause):
        if(bool(re.search("=", WhereClause)) or  bool(re.search(">", WhereClause)) or bool(re.search("<", WhereClause))):
            if(bool(re.search("=", WhereClause))):
                temp = WhereClause.split("=")
                result = True
                for part in temp:
                    if not part.strip():
                        result = False
                        break
                if not (result):
                    print("Incorrect Where Clause")
                    self.syntaxStatus = False

        else:
            print("Incorrect Where Clause")
            self.syntaxStatus = False
        '''