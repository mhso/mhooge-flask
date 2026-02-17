import os
import sys
import re
import inspect
import logging as stdlib_logging

from loguru import logger

class StdErrRedirect(object):
    def __init__(self, stderr_stream):
        self.stderr_stream = stderr_stream
        self.prev_msg_handshake = False
        self.gevent_error_lines = []

    def write(self, msg):
        ssl_error = re.search(r"ssl\..*Error", msg) is not None

        if ssl_error or self.gevent_error_lines != []:
            if "self._sslobj.do_handshake()" in msg:
                self.prev_msg_handshake = True

            if self.prev_msg_handshake:
                joined = "\n".join(self.gevent_error_lines)
                WSGI_ERROR_LOGGER.write(joined, ssl_error=True)
                self.gevent_error_lines = []
            else:
                self.gevent_error_lines.append(msg.replace("\n", ""))
        else:
           self.stderr_stream.write(msg)

        self.prev_msg_handshake = False

    def flush(self):
        pass

    def close(self):
        self.stderr_stream.close()

class WSGILogger(object):
    def __init__(self, level):
        self.level = level

    def write(self, msg, **bindings):
        if msg and msg.endswith('\n'):
            msg = msg[:-1]

        split = msg.split(" ")

        if self.level == WSGI_INFO_LEVEL:
            try:
                first_citation = msg.index("\"")
                after_first_cit = msg[first_citation + 1:]
                second_citation = after_first_cit.index("\"")

                info_part = after_first_cit[:second_citation]
                split = info_part.split(" ")

                if split[0].strip() not in ("GET", "POST", "PUT", "DELETE", "HEAD"):
                    return

                msg = msg[first_citation:]

            except ValueError:
                pass

        bindings["wsgi"] = self.level

        logger.bind(**bindings).log(self.level, msg)

    def close(self):
        super().close()

    def flush(self):
        pass

class SocketIOLoggingHandler(stdlib_logging.Handler):
    _RECIEVE_EVENTS_TO_FILER = ["calculate_ping", "ping_request"]
    _EMIT_EVENTS_TO_FILTER = ["ping_calculated", "ping_response"]

    def emit(self, record: stdlib_logging.LogRecord) -> None:
        message = record.getMessage()

        # Filter built-in PING/PONG messages
        if "Sending packet PING data" in message or "Received packet PONG data" in message:
            return

        # Filter custom recieve events
        for event in SocketIOLoggingHandler._RECIEVE_EVENTS_TO_FILER:
            if message.startswith(f'received event "{event}"') or re.search(rf'Received packet MESSAGE data .+\,\[\"{event}\"', message) is not None:
                return

        # Filter custom emit events
        for event in SocketIOLoggingHandler._EMIT_EVENTS_TO_FILTER:
            if message.startswith(f'emitting event "{event}"') or re.search(rf'Sending packet MESSAGE data .+\,\[\"{event}\"', message) is not None:
                return

        # Get corresponding Loguru level if it exists.
        level: str | int
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        # Find caller from where originated the logged message.
        frame, depth = inspect.currentframe(), 0
        while frame and (depth == 0 or frame.f_code.co_filename == stdlib_logging.__file__):
            frame = frame.f_back
            depth += 1

        logger.opt(depth=depth, exception=record.exc_info).log(level, message)

class SocketIOLogger(stdlib_logging.Logger):
    def __init__(self, name: str, level: int | str = 0) -> None:
        super().__init__(name, level)
        self.handlers = [SocketIOLoggingHandler(level)]

cwd = os.getcwd()
if cwd.endswith("src") or cwd.endswith("src/"):
    LOG_FOLDER = os.path.join(cwd, "..", "log")
else:
    LOG_FOLDER = os.path.join(cwd, "log")

if not os.path.exists(LOG_FOLDER):
    os.mkdir(LOG_FOLDER)

MAX_LOG_SIZE = "10MB"

# Application logging
APPLICATION_LOG_FILE = f"{LOG_FOLDER}/log.log"

# WSGI server logging
WSGI_INFO_LEVEL = "WSGI_INFO"
WSGI_ERROR_LEVEL = "WSGI_ERROR"

WSGI_ERROR_FILE = f"{LOG_FOLDER}/wsgi_error.log"

def initialize_logging():
    try:
        # Tests if logger has been initialized already.
        logger.level(WSGI_INFO_LEVEL)
        return
    except ValueError:
        pass

    time_fmt = "{time:YYYY-MM-DD HH:mm:ss}"
    stream_formatting = "<level>[{level}]</level> | <bold>\"{module}.{name}\", line {line}</bold> - <level>{message}</level>"

    app_info_format = f"<green>{time_fmt}</green> {stream_formatting}"
    app_warning_format = f"<yellow>{time_fmt}</yellow> {stream_formatting}"
    wsgi_info_format = f"<cyan>{time_fmt}</cyan> {stream_formatting}"
    wsgi_error_format = time_fmt + " {message}"
    error_format = f"<red>{time_fmt}</red> {stream_formatting}"

    logger.remove()

    # Redirect stderr to class for more control.
    stderr_stream = sys.stderr
    sys.stderr = StdErrRedirect(stderr_stream)

    # Create and update logging levels.
    logger.level(WSGI_INFO_LEVEL, no=21, color="<normal>")
    logger.level(WSGI_ERROR_LEVEL, no=41, color="<red>")
    logger.level("WARNING", color="<yellow>")

    # Add WSGI stdout debug logger.
    logger.add(
        sys.stdout,
        level=WSGI_INFO_LEVEL,
        filter=lambda record: record["level"].no < logger.level("WARNING").no,
        colorize=True,
        format=wsgi_info_format
    )

    # Add WSGI file error logger.
    logger.add(
        WSGI_ERROR_FILE,
        level=WSGI_ERROR_LEVEL,
        filter=lambda record: "wsgi" in record["extra"] and "ignore" not in record["extra"],
        rotation=MAX_LOG_SIZE,
        format=wsgi_error_format,
        retention="1 month",
        encoding="utf-8",
        serialize=True
    )

    # Add application stdout info logger.
    logger.add(
        sys.stdout,
        level="INFO",
        filter=lambda record: record["level"].no <= logger.level("INFO").no,
        colorize=True,
        format=app_info_format
    )

    # Add application stdout warning logger.
    logger.add(
        sys.stdout,
        level="WARNING",
        filter=lambda record: record["level"].no < logger.level(WSGI_ERROR_LEVEL).no,
        colorize=True,
        format=app_warning_format
    )

    # Add application stderr error logger.
    logger.add(
        stderr_stream,
        level="ERROR",
        filter=lambda record: "ssl_error" not in record["extra"],
        colorize=True,
        format=error_format
    )

    # Add application file debug logger.
    logger.add(
        APPLICATION_LOG_FILE,
        level="DEBUG",
        filter=lambda record: "wsgi" not in record["extra"] and "ignore" not in record["extra"],
        rotation=MAX_LOG_SIZE,
        retention="1 month",
        encoding="utf-8",
        serialize=True
    )

# Initialize logging on import, so it's always done immediately in a new process.
initialize_logging()

WSGI_INFO_LOGGER = WSGILogger(WSGI_INFO_LEVEL)
WSGI_ERROR_LOGGER = WSGILogger(WSGI_ERROR_LEVEL)
