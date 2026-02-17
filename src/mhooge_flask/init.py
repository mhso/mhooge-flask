import json
import importlib
import sys
import os
from dataclasses import dataclass
from abc import ABC, abstractmethod
from typing import Any, Dict
from gevent.pywsgi import WSGIServer
from werkzeug.middleware.proxy_fix import ProxyFix

from flask import Flask
from loguru import logger

from flask_cors import CORS
from flask_socketio import SocketIO

from . import logging
from . import routing
from .database import Database

@dataclass
class Route:
    route: str
    blueprint: str
    prefix: str = "root"
    folder: str = "routes"
    parent_route: "Route" = None

class ServerWrapper(ABC):
    def __init__(self, host: str, port: int, app: Flask):
        self.host = host
        self.port = port
        self.app = app
        self.handler = self.create_handler()

    @abstractmethod
    def create_handler(self) -> WSGIServer | SocketIO:
        ...

    @abstractmethod
    def run(self):
        ...

class WSGIServerWrapper(ServerWrapper):
    def create_handler(self):
        return WSGIServer(
            (self.host, self.port),
            self.app,
            log=logging.WSGI_INFO_LOGGER,
            error_log=logging.WSGI_ERROR_LOGGER,
        )

    def run(self):
        self.handler.serve_forever()

class SocketIOPatcher(SocketIO):
    def run(self, app, host: str | None = None, port: int | None = None, **kwargs):
        if host is None:
            host = '127.0.0.1'
        if port is None:
            server_name = app.config['SERVER_NAME']
            if server_name and ':' in server_name:
                port = int(server_name.rsplit(':', 1)[1])
            else:
                port = 5000

        app.debug = kwargs.pop('debug', app.debug)

        from gevent import pywsgi

        self.wsgi_server = pywsgi.WSGIServer((host, port), app, **kwargs)
        self.wsgi_server.serve_forever()

    def _handle_event(self, handler, message, namespace, sid, *args):
        try:
            super()._handle_event(handler, message, namespace, sid, *args)
        except:
            logger.exception("SocketIO error")

class SocketIOServerWrapper(ServerWrapper):
    def create_handler(self):
        return routing.socket_io

    def run(self):
        wsgi_args = {
            "log": logging.WSGI_INFO_LOGGER,
            "error_log": logging.WSGI_ERROR_LOGGER,
        }

        self.handler.run(
            self.app,
            self.host,
            self.port,
            **wsgi_args
        )

def create_app(
    app_name: str,
    root: str,
    routes: list[Route],
    database: Database,
    root_folder: str = "app",
    static_folder: str = "static",
    template_folder: str = "templates",
    server_cls: type[ServerWrapper] | None = WSGIServerWrapper,
    persistent_variables: Dict[str, Any] = {},
    **kw_config
):
    """
    Create a Flask application.

    :param app_name:        Name of the Flask Application
    :param root:            Root URL prefix of all endpoints
    :param routes:          List of Route instances that will each be associated
                            with a Flask Blueprint
    :param database:        SQLiteDatabase instance wherein user information
                            and metadata may be saved
    :param root_folder:     Root folder of the Flask application
    :param static_folder:   Static folder used by the Flask application
    :param template_folder: Template folder used by the Flask application
    """
    root_full_path = f"{os.getcwd()}/{root_folder}"

    web_app = Flask(
        app_name,
        root_path=root_full_path,
        static_folder=static_folder,
        template_folder=template_folder
    )

    CORS(web_app)
    web_app.wsgi_app = ProxyFix(web_app.wsgi_app, 1, 1, 1, 1)
    if server_cls is SocketIOServerWrapper:
        socket_io_logger = logging.SocketIOLogger("socket_io", "INFO")
        routing.socket_io = SocketIOPatcher(
            cors_allowed_origins="*",
            logger=socket_io_logger,
            engineio_logger=socket_io_logger,
        )

    # Set up the blueprints for all the pages/routes.
    route_modules = {}
    root_module = root_folder.replace("\\", "/").replace("/", ".")
    for route in routes:
        name = route.route
        path = route.folder
        route_modules[route.blueprint] = importlib.import_module(f"{root_module}.{path}.{name}")

        if route.parent_route is not None:
            name = route.parent_route.route
            path = route.parent_route.folder

            if route.parent_route.blueprint not in route_modules:
                route_modules[route.parent_route.blueprint] = importlib.import_module(f"{root_module}.{path}.{name}")

    parent_blueprints = set()

    # Register blueprints for all the pages/routes.
    for route in routes:
        module = route_modules[route.blueprint]
        if route.parent_route is None:
            prefix = root if route.prefix == "root" else f"{root}{route.prefix}"
        else:
            prefix = None if route.prefix == "root" else f"{route.prefix}"

        if route.parent_route is not None:
            parent_route = route.parent_route
            parent_module = route_modules[parent_route.blueprint]
            parent_prefix = root if parent_route.prefix == "root" else f"{root}{parent_route.prefix}"
            parent = getattr(parent_module, parent_route.blueprint)
            parent_blueprints.add((parent, parent_prefix))

        else:
            parent = web_app

        blueprint = getattr(module, route.blueprint)

        parent.register_blueprint(blueprint, url_prefix=prefix)

    for blueprint, prefix in parent_blueprints:
        web_app.register_blueprint(blueprint, url_prefix=prefix)

    # Set up Flask lifetime variables.
    for key in kw_config:
        web_app.config[key.upper()] = kw_config[key]

    secret_file = f"{root_full_path}/{static_folder}/secret.json"

    # Set up miscellaneous stuff
    web_app.config["DATABASE"] = database
    web_app.config["PERSISTENT_DATA"] = persistent_variables
    web_app.config["APP_ENV"] = "production" if sys.platform == "linux" else "dev"
    web_app.config["SECRET_FILE"] = secret_file
    web_app.config["_SERVER_CLS"] = server_cls

    # Load secret key for Flask app.
    try:
        with open(secret_file, "r", encoding="utf-8") as fp:
            web_app.secret_key = json.load(fp)["app_secret"]
    except (FileNotFoundError, KeyError):
        print(
            "Flask app key not found. Place it in "
            f"'{secret_file}' with key 'app_secret'."
        )

    if server_cls is SocketIOServerWrapper:
        routing.socket_io.init_app(web_app)

    return web_app

def run_app(app: Flask, app_name: str, port: int, host: str = ""):
    host_ip = "127.0.0.1" if host == "" else host
    logger.info(f"Starting Flask app on {host_ip}:{port}/{app_name}")

    server_cls = app.config["_SERVER_CLS"]

    # Run the server using the given server wrapper class.
    server = server_cls(host, port, app)

    del app.config["_SERVER_CLS"]

    server.run()

def set_persistent_data(app: Flask, data):
    app.config["PERSISTENT_DATA"] = data
