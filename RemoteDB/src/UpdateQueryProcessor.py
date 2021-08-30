import re
from RemoteDB.src.ParsedData import *


class UpdateQueryProcessor:

    def __init__(self, queryObj):
        self.queryObj = queryObj
        query = queryObj[0]
        self.ogQuery = query.strip()
        self.query = query.upper().strip()
        self.syntaxStatus = True
        self.response = ""

    def execute(self):
        self.parseAndCheck()

        if self.syntaxStatus:
            self.syntaxStatus = self.checkWhereTupleDataType()

        if self.syntaxStatus:
           self.syntaxStatus = self.checkUpdateSetTupleDataType()

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

    def parseAndCheck(self):
        WhereClause = ""
        if (bool(re.match("UPDATE +[A-Za-z0-9_]+(\s)+SET((\s)+[A-Za-z0-9_]+(\s)*=(\s)*(\"|')?[A-Za-z0-9\s]*(\"|')?,?)+(\s)+WHERE((\s)+[A-Za-z0-9_]+(\s)*=(\s)*(\"|')?[A-Za-z0-9,._\s]*(\"|')? *(AND|OR)?)+(;)?", self.query))):
            queryType = self.query.split(" ", 1)[0]

            temp = self.query.split(" ", 1)[1]
            TableName = temp.split(" ", 1)[0]

            if ('SET' in self.ogQuery):
                temp = self.ogQuery.split("SET ", 1)[1]
            elif ('set' in self.ogQuery):
                temp = self.ogQuery.split("SET ", 1)[1]

            if ('where' in self.ogQuery):
                temp = temp.split(" where", 1)[0]
            elif ('WHERE' in self.ogQuery):
                temp = temp.split(" WHERE", 1)[0]

            UpdateSet = temp.strip()

            if ('where' in self.ogQuery):
                temp = self.ogQuery.split(" where", 1)[1]
            elif ('WHERE' in self.ogQuery):
                temp = self.ogQuery.split(" WHERE", 1)[1]

            WhereClause = temp.strip()
            if (WhereClause[-1] == ";"):
                WhereClause = WhereClause[:-1].strip()
            if (len(WhereClause) == 0):
                self.syntaxStatus = False
                print("Missing Where Clause")

            temp = self.query.split("SET ", 1)[1]
            temp = temp.split(" WHERE", 1)[0]
            if(self.checkWhereClause(temp)):
                temp = re.split("'|\",.*?'|\"",temp)
                columnName=temp
                d=[]
                for i in range(0, len(a)):
                    if i % 2:
                        d.append(a[i])
                columnValue=d

            self.parsedData = ParsedData(
                queryType, TableName, "", "", WhereClause, UpdateSet)


        else:
            self.syntaxStatus = False

    def checkWhereClause(self, whereClause):

        if(bool(re.search("=", whereClause)) or  bool(re.search(">", whereClause)) or bool(re.search("<", whereClause))):
            if(bool(re.search("=", whereClause))):
                temp = whereClause.split("=")
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

    def checkWhereTupleDataType(self):
        self.parsedData.WhereClause = (self.parsedData.parseWhereClause(self.parsedData.WhereClause))
        status = self.parsedData.checkTupleDataType(self.parsedData.TableName, self.parsedData.WhereClause)
        if not status:
            self.response = self.response + "Datatype Mis-match Error \n"
        return (status)

    def checkUpdateSetTupleDataType(self):
        self.parsedData.UpdateSet = (self.parsedData.parseWhereClause(self.parsedData.UpdateSet))
        status = self.parsedData.checkTupleDataType(self.parsedData.TableName, self.parsedData.UpdateSet)
        if not status:
            self.response = self.response + "Datatype Mis-match Error \n"
        return (status)

    def checkSetClause(self,setClause):
        commaSeperatedKY=re.split(",")
        for i in range(0,len(commaSeperatedKY)):
            if not (bool(re.match(" ?[A-Za-z0-9_]+( )?=( )?(\"|')[A-Za-z ]*(\"|')",commaSeperatedKY[i]))):
                break
                self.syntaxStatus=False