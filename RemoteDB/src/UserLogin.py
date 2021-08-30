import os
import json
from hashlib import sha256


class UserLogin:

    def __init__(self):
        self.ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
        self.CREDENTIAL_FILE = os.path.join(self.ROOT_DIR, "Database/users.json")


    def verify(self, username, password):
        hashed_pass = sha256(password.encode("utf-8")).digest().hex()
        with open(self.CREDENTIAL_FILE) as f:
            user_list = json.loads(f.read())
        dbUser = None
        try:
            dbUser = next(filter(lambda user: user["username"]==username and user["password"]==hashed_pass, user_list))
        except StopIteration:
            pass

        if not dbUser:
            return False
        return True
