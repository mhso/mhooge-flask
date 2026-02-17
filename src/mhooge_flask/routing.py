import json

import flask
from flask_socketio import SocketIO

socket_io: SocketIO = None

def set_cookie(key, value, max_age, response):
    path = flask.current_app.name

    response.set_cookie(key, value, max_age, path=f"/{path}")

def make_template_context(template, status=200, **variables):
    variables.update(flask.current_app.config["PERSISTENT_DATA"])
    return flask.render_template(template, **variables), status

def make_json_response(data, http_code):
    if not isinstance(data, dict):
        data = {"response": str(data)}

    resp = flask.Response(response=json.dumps(data), status=http_code, mimetype="application/json")
    resp.headers["Content-Type"] = "application/json; charset=utf-8"
    return resp

def make_text_response(text, status_code):
    resp = flask.Response(response=text, status=status_code, mimetype="text/raw")
    resp.headers["Content-Type"] = "text/raw; charset=utf-8"
    return resp
