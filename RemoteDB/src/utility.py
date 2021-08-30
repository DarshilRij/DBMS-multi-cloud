from RemoteDB.src.GeneralLogger import *
from RemoteDB.src.EventLogger import *
import json
import requests
import os

class utility:
    def __init__(self):
        self.GL = GeneralLogger()
        self.EL = EventLogger()
        self.ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
        self.LOCK_MANAGER_FILE_PATH = os.path.join(self.ROOT_DIR,"Database/Dictionaries/LockManager.json")


    def getCurrentLocation(self):
        with open(self.ROOT_DIR + "/Database/Dictionaries/LocalDataDictionary.json", 'r') as readFile:
            data = json.load(readFile)
            return data["DatabaseLocation"]

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

    def readTableLockData(self) -> list:
        data = None
        with open(self.LOCK_MANAGER_FILE_PATH, "r") as f:
            data = f.read()
        if data:
            data = json.loads(data)
        else:
            data = list()
        return data

    def readTransactionLockTable(self, transactionID) -> list:
        isLocked = True
        lockedTables = self.readTableLockData()
        locklist = []
        index = -1
        for tranLock in lockedTables:
            if tranLock["transactionID"] == transactionID:
                index = lockedTables.index(tranLock)
                break
        if index > -1:
            locklist = lockedTables[index]["lockList"]
        return locklist

    def lockTable(self, tableName,transactionID):
        tableName = tableName.upper().strip()
        lockList = self.readTableLockData()
        strlockList = json.dumps(lockList)
        if not transactionID in strlockList:
            lock = []
            lock.append(tableName)
            temp = {
                'transactionID': transactionID,
                'lockList' : lock
            }
            lockList.append(temp)
        else:
            index = -1
            for tranLock in lockList:
                if tranLock["transactionID"] == transactionID:
                    index = lockList.index(tranLock)
                    break
            if index > -1:
                lock = lockList[index]["lockList"]
                if not tableName in lock:
                    lock.append(tableName)
                lockList[index]["lockList"] = lock
        #lockList = list(set(lockList))
        with open(self.LOCK_MANAGER_FILE_PATH, "w") as f:
            f.write(json.dumps(lockList))

    def releaseLock(self, transactionID):
        lockList = self.readTableLockData()
        try:
            index = -1
            for tranLock in lockList:
                if tranLock["transactionID"] == transactionID:
                    index = lockList.index(tranLock)
                    break
            if index > -1:
                del lockList[index]
        except ValueError:
            pass
        with open(self.LOCK_MANAGER_FILE_PATH, "w") as f:
            f.write(json.dumps(lockList))

    def isTableLocked(self, tableName: str, transactionID) -> bool:
        isLocked = True
        lockedTables = self.readTableLockData()
        strlockedTables = json.dumps(lockedTables)
        if tableName in strlockedTables:
            index = -1
            for tranLock in lockedTables:
                if tranLock["transactionID"] == transactionID:
                    index = lockedTables.index(tranLock)
                    break
                else:
                    return True
            if index > -1:
                if tableName in lockedTables[index]["lockList"]:
                    return False
                else:
                    return True
            else:
                return False
        else:
            return False

    def tablePath(self, tableName: str, transactionID) -> bool:
        isLocked = True
        lockedTables = self.readTableLockData()
        strlockedTables = json.dumps(lockedTables)
        if tableName in strlockedTables:
            index = -1
            for tranLock in lockedTables:
                if tranLock["transactionID"] == transactionID:
                    index = lockedTables.index(tranLock)
                    break
            if index > -1:
                if tableName in lockedTables[index]["lockList"]:
                    return "TempTables"
                else:
                    return "Tables"
            else:
                return "Tables"
        else:
            return "Tables"
