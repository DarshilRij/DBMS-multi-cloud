import json
import os
import datetime

class GeneralLogger:
    def __init__(self):
        self.ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

    def logMessage(self, msg):
        ts = datetime.datetime.now()

        msg = str(ts) + ">>>  " + msg
        with open(self.ROOT_DIR + "/Database/Logs/GeneralLogs.log", 'a') as writeFile:
            writeFile.write(msg + "\n")

    def writeDBState(self, queryType, TableName, noRows):
        msg = queryType + " query changed the Table " + TableName
        self.logMessage(msg)
        msg = "Currently Table "+ TableName +" has " + noRows + " number of Row(s)"
        self.logMessage(msg)


        
