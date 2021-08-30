from src.InsertQueryProcessor import *
from src.SelectQueryProcessor import *
from src.DeleteQueryProcessor import DeleteQueryProcessor
from src.UpdateQueryProcessor import *
from src.GeneralLogger import *
from src.EventLogger import *
from src.DataProcessor import *
from src.utility import *
from src.writeData import *
import datetime
import uuid
import json
import requests
import os


class QueryExecutor:
    def __init__(self):
        self.GL = GeneralLogger()
        self.EL = EventLogger()
        self.ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

    def execute(self):
        exitQueryExecutor = False
        while (not exitQueryExecutor):
            print("=======================================================================")
            print("=======================================================================")
            print("=======================================================================")

            print("*Note: Enter 0 to go back to main menu")
            print("Enter the query")
            val = input()
            val = val.strip()
            if (val):
                if(val == "0"):
                    exitQueryExecutor = True
                elif ("START TRANSACTION" in val.upper()):
                    self.Transaction()
                else:
                    self.singleQuery(val)
            
    def Transaction(self):
        transactionID = str(uuid.uuid4())
        endTransactionFlag = True
        TransactionCompleteFlag = True
        try:
            msg = "Started Creating Transaction"
            self.EL.logMessage(msg)
            
            transactionObj = []
            transactionID = str(uuid.uuid4())
            while(endTransactionFlag):
                val = input(">>>  ")
                if (val):
                    if not (("COMMIT" in val.upper()) or ("ROLLBACK" in val.upper())):
                        queryObj = []
                        queryObj.append(val)    #0-query string
                        queryObj.append(transactionID)    #1-transaction ID  
                        queryObj.append(None)    #2-location
                        queryObj.append(None)    #3-status
                        queryObj.append(None)    #4-parsed Obj
                        queryObj.append(None)    #5-response
                        transactionObj.append(queryObj)
                    else:
                        queryObj = []
                        if ("COMMIT" in val.upper()):
                            queryObj.append("COMMIT")    #0-query string
                        if ("ROLLBACK" in val.upper()):
                            queryObj.append("ROLLBACK")    #0-query string
                        queryObj.append(transactionID)    #1-transaction ID  
                        queryObj.append(None)    #2-location
                        queryObj.append(None)    #3-status
                        queryObj.append(None)    #4-parsed Obj
                        queryObj.append(None)    #5-response
                        transactionObj.append(queryObj)
                        endTransactionFlag = False

            for queryObj in transactionObj:
                queryObj = self.checkQuery(queryObj)
                if not (("COMMIT" in queryObj[0].upper()) or ("ROLLBACK" in queryObj[0].upper())):
                    
                    if not (queryObj[3]):
                        print("Transactioin Failed.")
                        msg = "Transaction: "+ transactionID +" Failed"
                        self.EL.logMessage(msg)
                        TransactionCompleteFlag =False
                        break
                    else:
                        index = transactionObj.index(queryObj)
                        obj = self.processQuery(queryObj)
                        transactionObj[index] = obj
                        if not obj[3]:
                            print (obj[5])
                            print("Transactioin Failed.")
                            TransactionCompleteFlag = False
                            msg = "Transaction: "+ transactionID +" Failed"
                            self.EL.logMessage(msg)
                            break
                else:
                    print()
        except Exception as e:
            self.EL.logMessage("Exception occured : "  + str(e))
        finally:
            TranLockList = utility().readTransactionLockTable(transactionID)
            if TransactionCompleteFlag:
                
                writeData().WriteTempData(TranLockList)
                
                print ("Transaction Completed successfully.")
                self.broadcastCommit(transactionID)
            utility().releaseLock(transactionID)
            self.broadcastReleaseLock(transactionID)
            writeData().clearTempTable(TranLockList)

    def singleQuery(self,val):
        #Create query object
        tranId = str(uuid.uuid4())
        querySuccess = True
        try:
            
            queryObj = []
            queryObj.append(val)    #0-query string
            queryObj.append(tranId)    #1-transaction ID  
            queryObj.append(None)    #2-location
            queryObj.append(None)    #3-status
            queryObj.append(None)    #4-parsed Obj
            queryObj.append(None)    #5-response
            queryObj = self.checkQuery(queryObj)
            if (queryObj[3]):
                queryObj = self.processQuery(queryObj)
                if not (queryObj[3]):
                    querySuccess =False
            else:
                querySuccess = False
        except Exception as e:
            self.EL.logMessage("Exception occured : "  + str(e))
        finally:
            temp= []
            temp.append(queryObj[4]["TableName"])
            if queryObj[5]:
                print(queryObj[5])
            if querySuccess and (queryObj[5]):
                temp= []
                temp.append(queryObj[4]["TableName"])
                writeData().WriteTempData(temp)
                self.broadcastCommit(tranId)
            utility().releaseLock(tranId)
            writeData().clearTempTable(temp)
            self.broadcastReleaseLock(tranId)
            

    def broadcastCommit(self, transactionID):
        queryObj = []
        queryObj.append("COMMIT")    #0-query string
        queryObj.append(transactionID)    #1-transaction ID  
        queryObj.append("REMOTE")    #2-location
        queryObj.append(None)    #3-status
        queryObj.append(None)    #4-parsed Obj
        queryObj.append(None)    #5-response
        self.processQuery(queryObj)

    def broadcastReleaseLock(self, transactionID):
        queryObj = []
        queryObj.append("RELEASE")    #0-query string
        queryObj.append(transactionID)    #1-transaction ID  
        queryObj.append("REMOTE")    #2-location
        queryObj.append(None)    #3-status
        queryObj.append(None)    #4-parsed Obj
        queryObj.append(None)    #5-response
        self.processQuery(queryObj)


    def processQuery(self,queryObj):
        parsedData = queryObj[4]
        if not (queryObj[2].strip().upper() and queryObj[2].strip().upper() == utility().getCurrentLocation() ):
            queryObj = self.callRemote(queryObj)
            if not queryObj[1]:
                print(queryObj[5])
            else:
                return queryObj
        else:
            dataProcessor =  DataProcessor(queryObj, parsedData)
            queryObj = dataProcessor.execute()
            if not queryObj[1]:
                print(queryObj[5])
            else:
                return queryObj

    def callRemote(self,queryObj):
        try:
            url = 'http://34.72.118.170:5000/execute-query'
            data = json.dumps(queryObj)
            response = requests.post(url, data=data)
            resData = (json.loads(response.text))
            return (resData["data"])
        except Exception as e:
            print (e)
            queryObj[3] = False
        return queryObj

    def checkQuery(self,queryObj):
        try:
            val = queryObj[0]
            msg = "Started executing query '" + val+ "'"
            self.EL.logMessage(msg)
            start_time = datetime.datetime.now()

            queryType = val.split(" ", 1)[0].upper()

            if queryType == "INSERT":
                insertQuery = InsertQueryProcessor(queryObj)
                queryObj = insertQuery.execute()
                self.EL.logMessage(queryType +" Query response is '" + queryObj[5] + "'")
            elif queryType == "UPDATE":
                updateQuery = UpdateQueryProcessor(queryObj)
                queryObj = updateQuery.execute()
                self.EL.logMessage(queryType +" Query response is '" + queryObj[5] + "'")
            elif queryType == "SELECT":
                selectQuery = SelectQueryProcessor(queryObj)
                queryObj = selectQuery.execute()
            elif queryType == "DELETE":
                deleteQuery = DeleteQueryProcessor(queryObj)
                queryObj = deleteQuery.execute()
                self.EL.logMessage(queryType +" Query response is '" + queryObj[5] + "'")
        except Exception as e:
            self.EL.logMessage("Exception occured : "  + str(e))
        finally:
            end_time = datetime.datetime.now()
            time_diff = (end_time - start_time)
            execution_time = time_diff.total_seconds() * 1000
            msg = "Query '"+ val +"' executed in "+ str(execution_time) + " msec"
            self.GL.logMessage(msg)
            return queryObj