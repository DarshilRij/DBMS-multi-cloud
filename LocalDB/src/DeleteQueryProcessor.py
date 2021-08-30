import re
import logging
from src.ParsedData import *


class DeleteQueryProcessor:

    logger = logging.getLogger(__name__)

    __DELETE_QUERY_REGEX = """
        \s*
        DELETE\s+
        FROM\s+
        (?P<table>[a-zA-Z0-9_-]+)
        \s*
        (?P<where_clause>
            WHERE\s+
            (?P<clause_list>
                (?P<clause1>
                    (?P<cmp_field1_name>
                        [a-zA-Z0-9_-]+)\s*
                        (?P<cmp_field1_operator>=|!=|>|<|>=|<=)\s*
                        (?P<cmp_field1_to>'[^']*'|[0-9]+|null)
                    )\s*
                    (?P<other_clauses>
                        (?P<clause_operator>and|or)\s+
                        (?P<clause2>(?P<cmp_field2_name>[a-zA-Z0-9_-]+)\s*
                        (?P<cmp_field2_operator>=|!=|>|<|>=|<=)\s*
                        (?P<cmp_field2_to>'[^']*'|[0-9]+|null)
                    )\s*
                )*
            )
        )?\s*
        ;"""

    __DELETE_QUERY_WHERE_CLAUSE_REGEX = """
        (?P<clause>
            (?P<field_name>[a-zA-Z0-9_-]+)\s*
            (?P<field_operator>=|!=|>|<|>=|<=)\s*
            (?P<field_cmp_value>'[^']*'|[0-9]+|null)
        )
        (
            \s+
            (?P<clause_operator>and|or)?
            \s+
        )?
    """


    def __init__(self, queryObj):
        self.queryObj = queryObj
        query = queryObj[0]
        self.origQuery = query.strip()
        self.isSyntaxValid = None
        self.response = ""
        # self.logger.setLevel(logging.DEBUG)

    def execute(self):
        parsedData = None
        if self.isSyntaxValid is None:
            parsedData = self.parse()
        if parsedData:
            '''
            print("DQP: ")
            print(parsedData.queryType)
            print(parsedData.TableName)
            print(parsedData.columnName)
            print(parsedData.columnValue)
            print(parsedData.WhereClause)
            '''
            parsedObj = {
                'transactionId': self.queryObj[1],
                'queryType': parsedData.queryType,
                'TableName': parsedData.TableName,
                'columnName': parsedData.columnName,
                'columnValue': parsedData.columnValue,
                'WhereClause': parsedData.WhereClause,
                'UpdateSet': parsedData.UpdateSet
            }
            self.queryObj[2] = parsedData.getTableLocation(parsedData.TableName)
            self.queryObj[3] = True
            self.queryObj[4] = parsedObj

        else :
            self.queryObj[2] = None
            self.queryObj[3] = False
            self.response = self.response + ("Invalid syntax \n")
            self.queryObj[5] = self.response
        return self.queryObj

    def parse(self):
        match = re.fullmatch(self.__DELETE_QUERY_REGEX, self.origQuery, re.IGNORECASE | re.VERBOSE)

        self.isSyntaxValid = match is not None

        if not self.isSyntaxValid:
            # early exit if invalid systax
            return

        table_name = match.group("table")
        has_where_clause = bool(match.group("where_clause"))
        # ((field, operator, value),'and|or',(field, operator, value)...)
        clause_list = list()
        for match in re.finditer(
            self.__DELETE_QUERY_WHERE_CLAUSE_REGEX, self.origQuery,
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
        queryType = self.origQuery.upper().split(" ", 1)[0]
        parsedData = ParsedData(queryType, table_name,"","", where_clause)
        return parsedData
