from time import time
import json
from sqlite3 import Connection, OperationalError, ProgrammingError, Cursor
from typing import List, Tuple
from sqlalchemy import Result, text
from sqlalchemy.orm import Session
from sqlalchemy.exc import ResourceClosedError

from mhooge_flask.database import Query, Database

def _try_cast(param):
    if param == "None":
        return None
    if param == "True":
        return True
    if param == "False":
        return False

    try:
        return int(param)
    except ValueError:
        try:
            return float(param)
        except ValueError:
            try:
                return json.loads(param)
            except json.JSONDecodeError:
                try:
                    return json.loads(param.replace("'", '"'))
                except json.JSONDecodeError:
                    return str(param)

def format_value(val):
    if isinstance(val, float):
        return f"{val:.4f}"

    if isinstance(val, str):
        return f'"{val}"'

    return str(val)

def format_raw_output(rows, columns=None):
    if rows == []:
        return 0

    all_rows = []
    row_lengths = []
    for row_index, row in enumerate(rows):
        if row_index == 0:
            if columns:
                row_lengths = [len(col[0]) for col in columns]
            else:
                row_lengths = [0] * len(row)

        for col_index, col in enumerate(row):
            data = format_value(col)
            row_lengths[col_index] = max(len(data), row_lengths[col_index])

        all_rows.append(row)

    if columns:
        row_fmt = []
        for col_index, col in enumerate(columns):
            padding = " " * (row_lengths[col_index] - len(col[0]))
            row_fmt.append(f"{col[0]}{padding}")

        print(" | ".join(row_fmt))
        print("-" * (sum(row_lengths) + len(columns) * 3))

    for row in all_rows:
        row_fmt = []
        for col_index, col in enumerate(row):
            data = format_value(col)

            padding = " " * (row_lengths[col_index] - len(data))
            row_fmt.append(f"{data}{padding}")

        print(" | ".join(row_fmt))

    return len(all_rows)

def format_output(output, raw):
    rows_returned = 0

    if raw:
        rows_returned = format_raw_output(output)
    else:
        if isinstance(output, list):
            for row in output:
                print(format_value(row))

            rows_returned = len(output)

        else:
            print(output)
            rows_returned = None

    return rows_returned

def run_query_function(database, query, *params, raw=False, print_query=False):
    if not query.endswith(".sql") and not hasattr(database, query):
        print("The query is not supported by the given database. Exiting...")
        exit(0)

    time_start = time()

    if query.endswith(".sql"):
        raw = False
        with database:
            with open(query) as fp:
                result = database.execute_query(fp.read())
    else:
        query_func = getattr(database, query)
        params = list(map(_try_cast, params))

        query_obj = query_func(*params)
        if isinstance(query_obj, Query):
            if print_query:
                print(query_obj)
                exit(0)

            result = query_obj(raw=raw)
        else:
            if print_query:
                raise TypeError(
                    f"The method '{query}' did not return a Query object and can't be printed."
                )

            result = query_obj
            raw = False

    rows_returned = format_output(result, raw)

    time_end = time()
    time_taken = f"{time_end - time_start:.3f} seconds."

    rows_affected = database.connection.cursor().execute("SELECT changes()").fetchone()[0]
    if rows_affected > 0:
        print(f"Rows affected: {rows_affected} in {time_taken}")
    elif rows_affected is None:
        print("Unknown rows affected.")
    else:
        print(f"Rows returned: {rows_returned} in {time_taken}")

def run_query_string(query: str, conn: Connection | Session):
    if isinstance(conn, Connection): # SQLite built-in
        return conn.cursor().execute(query)

    return conn.execute(text(query)) # SQLAlchemy

def get_column_names(result: Cursor | Result):
    if isinstance(result, Cursor): # SQLite built-in
        return result.description

    try:
        return [[str(key)] for key in result.keys()] # SQLAlchemy
    except ResourceClosedError:
        return []

def extract_rows(result: Cursor | Result) -> List[Tuple[str]]:
    if isinstance(result, Cursor): # SQLite built-in
        return result

    try:
        return [] if result.closed else result.fetchall() # SQLAlchemy
    except ResourceClosedError:
        return []

def get_rows_affected(conn: Connection | Session):
    if isinstance(conn, Connection): # SQLite built-in
        return conn.cursor().execute("SELECT changes()").fetchone()[0]
    
    return conn.execute(text("SELECT changes()")).scalar_one_or_none()

def query_or_repl(
    database: Database,
    query: list[str] = None,
    raw: bool = False,
    print_query: bool = False
):
    with database:
        # Execute a single given query
        if query is not None:
            if len(query) > 1 and not query[0].endswith(".sql"):
                query, *params = query
            else:
                query = query[0]
                params = []

            run_query_function(database, query, *params, raw=raw, print_query=print_query)

        else:
            # Launch REPL-like loop
            conn = database.connection
            while True:
                try:
                    query = input(">")
                    if query in ("q", "quit", "exit"):
                        break

                    if query.startswith("run"):
                        _, query, *params = query.split(" ")
                        run_query_function(database, query, *params, raw=True)
                        continue

                    time_start = time()

                    try:
                        result = run_query_string(query, conn)
                    except Exception as exc:
                        print("Error during query:", str(exc))
                        continue
                    
                    column_names = get_column_names(result)
                    rows = extract_rows(result)

                    rows_returned = format_raw_output(rows, column_names)

                    conn.commit()
                    time_end = time()
                    time_taken = f"{time_end - time_start:.3f} seconds."

                    if rows_returned != 0:
                        print(f"Rows returned: {rows_returned} in {time_taken}")
                    else:
                        rows_affected = get_rows_affected(conn)
                        print(f"Rows affected: {rows_affected} in {time_taken}")

                except (OperationalError, ProgrammingError) as exc:
                    print(exc.args)
                except KeyboardInterrupt:
                    pass
