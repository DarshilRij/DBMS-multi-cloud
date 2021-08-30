import json
import os
import datetime

class EventLogger:
    def __init__(self):
        self.ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
    
    def logMessage(self,msg):
        ts = datetime.datetime.now()

        msg = str(ts) + ">>>  " + msg
        with open(self.ROOT_DIR + "/Database/Logs/EventLogs.log", 'a') as writeFile:
            writeFile.write(msg + "\n")