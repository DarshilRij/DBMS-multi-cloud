from flask import Flask, request, current_app
from src.QueryExecutor import QueryExecutor
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

app.run()
