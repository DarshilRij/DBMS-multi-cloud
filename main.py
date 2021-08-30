from flask import Flask, request, current_app
from RemoteDB.src.QueryExecutor import QueryExecutor
import json


app = Flask(__name__)

@app.route("/", methods=["GET", "POST"])
def index():
    resp = {
        "success": True,
        "message": "hello world",
        "data": {},
    }
    return resp


@app.route("/execute-query", methods=["POST"])
def execute_query():
    request_body = request.data
    current_app.logger.error("request body: %s", request_body)
    if not request_body:
        return (
            {
                "success": False,
                "message": "Invalid request body",
                "data": {}
            },
            400,
        )
    data = request_body.decode('utf-8')
    query = json.loads(data)
    query_executor = QueryExecutor()
    if query[0] == "COMMIT":
        result = query_executor.comitChanges(query)
    elif query[0] == "RELEASE":
        result = query_executor.releaseLock(query)
    else:
        result = query_executor.processQuery(query)

    current_app.logger.error("query result: %s", result)

    return {
        "success": True,
        "message": "Query executed",
        "data": result,
    }


@app.route("/get-local-dictionary", methods=["GET"])
def get_local_dictionary():
    file_data = None
    with open("RemoteDB/src/Database/Dictionaries/LocalDataDictionary.json", 'r') as dd:
        file_data = dd.read()
    json_data = dict()
    if file_data:
        json_data = json.loads(file_data)

    return {
        "success": True,
        "message": "Local data dictionary",
        "data": json_data
    }

if __name__ == "__main__":
    app.run(host="0.0.0.0")
