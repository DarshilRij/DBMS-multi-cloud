import re
import os
import json
import requests

class ParsedData:
    def __init__(self, queryType, TableName, columnName, columnValue, WhereClause, UpdateSet=None):
        self.queryType = queryType
        self.TableName = TableName
        self.columnName = columnName
        self.columnValue = columnValue
        self.WhereClause = WhereClause
        self.UpdateSet = UpdateSet
        self.ROOT_DIR = os.path.dirname(os.path.abspath(__file__))


    def getTableLocation(self, tableName:str):
        #print("DP: "+self.parsedData.TableName.upper())
        with open(self.ROOT_DIR + "/Database/Dictionaries/GlobalDataDictionary.json", 'r') as readFile:
            data = json.load(readFile)
            tableLocation = ""
            for d in data:
                if (d.get("TableName").upper() == tableName.upper() ) :
                    tableLocation = d.get("Location").upper()
                    break
        return tableLocation

    @staticmethod
    def parseWhereClause(where_clause: str):
        DELETE_QUERY_WHERE_CLAUSE_REGEX = """
            (?P<clause>
                (?P<field_name>[a-zA-Z0-9_-]+)\s*
                (?P<field_operator>=|!=|>|<|>=|<=)\s*
                (?P<field_cmp_value>'[^']*'|[0-9]+|null)
            )
            (
                \s+
                (?P<clause_operator>and|or)?
                \s+
            )?"""

        clause_list = list()

        for match in re.finditer(
            DELETE_QUERY_WHERE_CLAUSE_REGEX, where_clause,
            re.IGNORECASE | re.VERBOSE,
        ):
            field_name = match.group("field_name")
            field_operator = match.group("field_operator")
            field_cmp_value = match.group("field_cmp_value")

            try:
                # try conversion to int
                field_cmp_value = int(field_cmp_value)
            except:
                pass
            if field_cmp_value == "null":
                field_cmp_value = None

            clause = (field_name, field_operator, field_cmp_value)

            clause_list.append(clause)

            clause_operator = match.group("clause_operator")
            if clause_operator:
                clause_list.append(clause_operator)

        where_clause = tuple(clause_list)

        return where_clause

    def checkDataType(self, tableName, column, values):
        status =True
        location = self.getTableLocation(tableName)
        data = []
        with open(self.ROOT_DIR + "/Database/Dictionaries/LocalDataDictionary.json", 'r') as readFile:
            data = json.load(readFile)
        foundTable = False
        for DD in data["TableDetails"]:
            if(tableName.upper().strip() == DD.get("TableName").upper().strip()):
                data = DD.get("ColumnDetails")
                foundTable = True
                break
        if foundTable:
            for col in column:
                colDetails = {}
                foundColumn = False
                for DD in data:
                    if(col.upper().strip() == DD.get("ColumnName").upper().strip()):
                        colDetails = DD
                        foundColumn = True
                        break
                
                if foundColumn:
                    index = column.index(col)
                    val = values[index]
                    datatype = colDetails.get("DataType")
                    if datatype.upper() == "VARCHAR(100)" or datatype.upper() == "TEXT":
                        if not (isinstance(val, str)):
                            status = False
                            break

                    elif datatype.upper() == "VARCHAR(100)":
                        m = re.search("\((.+?)\)", datatype)
                        if m:
                            length = m.group(1)
                        if len(val) > int(length):
                            status = False
                            break
                    elif datatype.upper() == "INT":
                        try:
                            i = int(val)
                        except ValueError as verr:
                            status = False
                            break
        return status

    def checkTupleDataType(self, tableName, tupleSet):
        status =True
        location = self.getTableLocation(tableName)
        data = []
        with open(self.ROOT_DIR + "/Database/Dictionaries/LocalDataDictionary.json", 'r') as readFile:
            data = json.load(readFile)
        foundTable = False
        for DD in data["TableDetails"]:
            if(tableName.upper().strip() == DD.get("TableName").upper().strip()):
                data = DD.get("ColumnDetails")
                foundTable = True
                break
        if foundTable:
            for col in tupleSet:
                colDetails = {}
                foundColumn = False
                for DD in data:
                    if(col[0].upper().strip() == DD.get("ColumnName").upper().strip()):
                        colDetails = DD
                        foundColumn = True
                        break
                if foundColumn:
                    index = tupleSet.index(col)
                    val = col[2]
                    datatype = colDetails.get("DataType")
                    if datatype.upper() == "VARCHAR(100)" or datatype.upper() == "TEXT":
                        if not (isinstance(val, str)):
                            status = False
                            break

                    elif datatype.upper() == "VARCHAR(100)":
                        m = re.search("\((.+?)\)", datatype)
                        if m:
                            length = m.group(1)
                        if len(val) > int(length):
                            status = False
                            break
                    elif datatype.upper() == "INT":
                        try:
                            i = int(val)
                        except ValueError as verr:
                            status = False
                            break
        return status

    @staticmethod
    def parseInsertValues(clause: str) -> tuple:
        KEY_VALUE_REGEX = """
            (?P<value>'[^']*'|\\b[0-9]+\\b|null)
            ,?
            """
        clause_list = list()
        for match in re.finditer(
            KEY_VALUE_REGEX, clause,
            re.IGNORECASE | re.VERBOSE,
        ):
            value = match.group("value")

            try:
                # try convertion to int
                value = int(value)
            except:
                pass
            if value == "null":
                value = None

            clause_list.append(value)
        return tuple(clause_list)
