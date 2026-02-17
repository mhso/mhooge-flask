import hashlib
import json
import random
from time import time

import flask

from . import routing

def generate_auth_token(user_id, secret_file):
    with open(secret_file, "r", encoding="utf-8") as fp:
        salt = json.load(fp)["auth_token_salt"]

    time_now = time()
    payload = f"{user_id}{salt}{time_now}"

    return hashlib.sha256(bytes(payload, encoding="utf-8")).hexdigest()

def user_exists(user):
    database = flask.current_app.config["DATABASE"]

    return database.get_user_id(user) is not None

def generate_user_id():
    return "".join(str(random.randint(0, 9)) for _ in range(32))

def get_hashed_password(password, secret_file):
    with open(secret_file, "r", encoding="utf-8") as fp:
       salt = json.load(fp)["password_salt"]

    return hashlib.sha256(bytes(password + salt, encoding="utf-8")).hexdigest()

def save_auth_token(user_id, resp, secret_file, max_age=60*60*24*60):
    database = flask.current_app.config["DATABASE"]
    auth_token = generate_auth_token(user_id, secret_file)

    app_name = flask.current_app.name

    database.save_auth_token(auth_token, user_id, max_age)

    routing.set_cookie(f"{app_name}_token", auth_token, max_age, resp)

    return resp

def verify_input(data, names, keys, lengths):
    for name, key, max_len in zip(names, keys, lengths):
        if key not in data:
            return False, f"{name} must be given."
        value = data[key]
        if value is None or value == "":
            return False, f"{name} must not be empty."
        if len(value) > max_len:
            return False, f"{name} must be a maximum of {max_len} characters in length."

    return True, None

def login(data, user_key, pass_key, url_success, template_error, auth_max_age=60*60*24*60, **redirect_params):
    database = flask.current_app.config["DATABASE"]
    secret_file = flask.current_app.config["SECRET_FILE"]

    if not user_key in data or not pass_key in data:
        error_msg = "Could not login: Username or Password was not given."
        return routing.make_template_context(template_error, 400, error=error_msg)

    hashed_password = get_hashed_password(data[pass_key], secret_file)

    if not database.password_matches(data[user_key], hashed_password):
        # Password or user doesn't exist/match.
        error_msg = "Could not login: Username or Password does not match."
        return routing.make_template_context(template_error, 401, error=error_msg)

    user_id = database.get_user_id(data[user_key])

    resp = flask.make_response(flask.redirect(flask.url_for(url_success, **redirect_params)))

    return save_auth_token(user_id, resp, secret_file, auth_max_age)

def signup(data, user_key, pass_key, url_success, template_error, auth_max_age=60*60*24*60):
    database = flask.current_app.config["DATABASE"]
    secret_file = flask.current_app.config["SECRET_FILE"]

    input_verified, error = verify_input(
        data, ["Username", "Password"], [user_key, pass_key], [32, 64]
    )

    if not input_verified:
        error_msg = f"Could not create account: {error}"
        return routing.make_template_context(template_error, 400, error=error_msg)

    user_id = generate_user_id()

    hashed_password = get_hashed_password(data[pass_key], secret_file)

    if database.create_user(user_id, data[user_key], hashed_password):
        resp = flask.make_response(flask.redirect(flask.url_for(url_success)))
        return save_auth_token(user_id, resp, secret_file, auth_max_age)

    # Username not unique.
    return routing.make_template_context(template_error, 400, error="Username is already taken.")

def get_user_details():
    database = flask.current_app.config["DATABASE"]

    app_name = flask.current_app.name
    auth_token = flask.request.cookies.get(f"{app_name}_token")

    if auth_token is None:
        return None

    return database.get_user_id_from_token(auth_token)
