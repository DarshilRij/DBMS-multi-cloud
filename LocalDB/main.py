from flask import Flask, request, current_app
from src.QueryExecutor import QueryExecutor

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
    request_body = request.json
    current_app.logger.info("request body: %s", request_body)
    if not request_body:
        return (
            {
                "success": False,
                "message": "Invalid request body",
                "data": {}
            },
            400,
        )

    query = request_body["query"]
    query_executor = QueryExecutor()
    result = query_executor.processQuery(query)

    current_app.logger.info("query result: %s", result)

    return {
        "success": True,
        "message": "Query executed",
        "data": result,
    }

app.run()
