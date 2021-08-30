from src.TransactionExecutor import *
from src.QueryExecutor import *
from src.erdGenerator import *
from src.sqlDumpGenerator import *
from src.UserLogin import UserLogin



loginExitFlag = False

while (not loginExitFlag):
    isValid = True
    username = input("enter username: ")
    password = input("enter password: ")
    user_login = UserLogin()
    if not user_login.verify(username, password):
        print("Invalid credentials")
        isValid = False


    if isValid:
        exitFlag = False
        while(not exitFlag):
            print("=======================================================================")
            print("=======================================================================")
            print("=======================================================================")
            print("Please select an action:")
            print("1. Enter Query")
            print("2. Generate ERD")
            print("3. Create SQL dump")
            print("0. Exit")
            val = input("Enter your selection: ")
            if(val == "0"):
                exitFlag = True
            elif(val == "1"):
                QueryExecutor().execute()
            elif(val == "2"):
                
                erdGenerator().execute()
            elif(val == "3"):
                sqlDumpGenerator().execute()

