from enum import Enum
import importlib
import os
import pkgutil
import re
import sqlite3
from shutil import copyfile
from sqlite3 import ProgrammingError, OperationalError, DatabaseError, Connection, Cursor
from threading import get_ident
from time import time
from typing import Dict, List, Tuple

from marshmallow.fields import Enum as MarshEnum
from marshmallow_sqlalchemy import SQLAlchemyAutoSchema, SQLAlchemySchema
from marshmallow_sqlalchemy.fields import Related, RelatedList
from sqlalchemy import Integer, create_engine, String, ForeignKey, delete, event, select, update
from sqlalchemy.orm import Session, Mapped, sessionmaker, DeclarativeBase, mapped_column, Mapper, RelationshipDirection, InstrumentedAttribute
from sqlalchemy.sql.schema import CallableColumnDefault
from pydantic import create_model, Field, BaseModel
from loguru import logger

class DBException(OperationalError, ProgrammingError):
    def __init__(self, *args):
        super().__init__(args)

class Query:
    def __init__(self, query: str, *params, format_func = None, default = None, context = None):
        collected_params = []
        for param in params:
            if type(param) in (list, tuple):
                collected_params.append(tuple(param))
            else:
                collected_params.append(param)

        execute_many = collected_params != [] and all(isinstance(param, tuple) for param in collected_params)

        if not execute_many:
            collected_params = tuple(collected_params)

        self.params = collected_params
        self.query = query
        self.execute_many = execute_many
        self.format_func = format_func
        self.default = default
        self.context = context

    def __str__(self):
        query = self.query
        for param in self.params:
            query = query.replace("?", str(param), 1)

        return query

    def __call__(self, commit: bool = True, raw: bool = False):
        with self.context as context:
            conn: Connection = context.connection
            try:
                if self.execute_many:
                    cursor = conn.cursor().executemany(self.query, self.params)
                else:
                    cursor = conn.cursor().execute(self.query, self.params)

                if commit:
                    conn.commit()

                if raw or self.format_func is None:
                    return cursor

                if callable(self.format_func):
                    return self.format_func(cursor)

                if self.format_func == "all":
                    return cursor.fetchall() or ([] if self.default is None else self.default)

                if self.format_func == "one":
                    return cursor.fetchone() or self.default

                if self.format_func == "unpack_all":
                    return [x[0] for x in cursor] or ([] if self.default is None else self.default)

                if self.format_func == "unpack_one":
                    val = cursor.fetchone()
                    return self.default if val is None else val[0]

                raise ValueError("Formatting function for query is invalid.", self.format_func)

            except (OperationalError, ProgrammingError, DatabaseError) as exc:
                raise DBException(exc.args)

class RelatedExtra(Related):
    def _serialize(self, value, attr, obj):
        if value is None:
            return None

        ret = {prop.key: getattr(value, prop.key, None) for prop in self.related_keys}
        ret.update(value.extra_fields)
        ret.update(value.dump())
        return ret if len(ret) > 1 else next(iter(ret.values()))

def _fix_type(data):
    if isinstance(data, dict):
        return {key: _fix_type(data[key]) for key in data}
    if isinstance(data, list):
        return [_fix_type(entry) for entry in data]

    if isinstance(data, Enum):
        return data.value

    return data

class Base(DeclarativeBase):
    __marsh__ = SQLAlchemySchema
    __pydantic__ = BaseModel
    __validate_fields__ = {}
    __serialize_relationships__ = []

    @property
    def extra_fields(self):
        return {}

    def dump(self, many: bool | None = None, included_relations: List[InstrumentedAttribute] | None = None, **key_mapping):
        marsh = self.__marsh__()

        if included_relations is not None:
            fields_to_include = set()
            for relationship in included_relations:
                field = relationship.key

                if field not in marsh.declared_fields:
                    continue

                if isinstance(marsh.declared_fields[field], (Related, RelatedList)):
                    fields_to_include.add(field)

            marsh.dump_fields = {
                field: marsh.declared_fields[field]
                for field in marsh.declared_fields
                if not isinstance(marsh.declared_fields[field], (Related, RelatedList)) or field in fields_to_include
            }

        data = marsh.dump(self, many=many)
        data_list = [data]
        if many:
            data_list = data

        for entry in data_list:
            for k, v in self.extra_fields.items():
                entry[k] = v

        # Map keys using 'key_mapping'
        for entry in data_list:
            for key in list(entry.keys()):
                if (out_key := key_mapping.get(key)):
                    entry[out_key] = entry[key]
                    del entry[key]

        data = list(map(_fix_type, data_list))

        return data if many else data[0]

class User(Base):
    __tablename__ = "users"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    name: Mapped[str] = mapped_column(String(32), unique=True)
    password: Mapped[str] = mapped_column(String(128))

class AuthToken(Base):
    __tablename__ = "auth_tokens"

    holder_id: Mapped[str] = mapped_column(String(64), ForeignKey("users.id"))
    token: Mapped[str] = mapped_column(String(64), primary_key=True)
    expires: Mapped[int] = mapped_column(Integer, nullable=False)

class Database:
    def __init__(self, db_path: str, add_user_tables: bool) -> None:
        self.db_path = db_path
        self.add_user_tables = add_user_tables
        self._connections = {}

    @property
    def connection(self) -> Connection | Session | None:
        return self._connections.get(get_ident())

    def get_connection(self) -> Connection | Session | None:
        ...

    def create_backup(self):
        without_ext = self.db_path.replace(".db", "")
        backup_name = f"{without_ext}_backup.db"
        try:
            # Remove old backup if it exists.
            if os.path.exists(backup_name):
                os.remove(backup_name)

            copyfile(self.db_path, backup_name)
        except (OSError, IOError) as exc:
            raise DBException(exc.args[0])

def setup_pydantic_models():
    for class_ in Base.__subclasses__():
        if not hasattr(class_, "__tablename__"):
            continue

        # Create dynamic pydantic schema
        validator_class_name = f"{class_.__name__}Validator"

        fields = {}
        for column in class_.__mapper__.columns:
            field_dict = {
                "validate_default": True if column.default is not None else False,
                "title": column.name,
                **class_.__validate_fields__.get(column.name, {}),
            }

            if column.default:
                if isinstance(column.default, CallableColumnDefault):
                    field_dict["default"] = column.default.arg(None)
                else:
                    field_dict["default"] = column.default.arg
            elif column.nullable:
                field_dict["default"] = None

            field_type = column.type.python_type | None if column.nullable else column.type.python_type

            if hasattr(column.type, "length") and not column.type.python_type.__base__ is Enum:
                field_dict["max_length"] = column.type.length

            fields[column.name] = (field_type, Field(**field_dict))

        # Find any field validate functions the model may define
        field_validators = {}
        for key in class_.__dict__:
            if (func_match := re.match(r"__(.+)_validator__", key)) and callable(class_.__dict__[key]):
                field_validators[func_match[1]] = class_.__dict__[key]

        validator_cls = create_model(validator_class_name, __base__=BaseModel, **fields)
        setattr(class_, "__pydantic__", validator_cls)

def setup_marshmallow_schemas(session):
    # Create a function which incorporates the Base and session information
    def setup_schema_fn():
        for class_ in Base.__subclasses__():
            if not hasattr(class_, "__tablename__"):
                continue

            # Create dynamic marshmallow schema
            class Meta(object):
                model = class_
                sqla_session = session

            schema_class_name = f"{class_.__name__}Schema"

            # Add relationships as 'Related' fields
            relations = {}
            for relationship in class_.__serialize_relationships__:
                name = relationship.key
                related_class = relationship.entity.class_
                cols = [col.name for col in related_class.__table__.columns]

                if relationship.direction in (RelationshipDirection.ONETOMANY, RelationshipDirection.MANYTOMANY):
                    relations[name] = RelatedList(RelatedExtra(cols))
                else:
                    relations[name] = RelatedExtra(cols)

            schema_class = type(
                schema_class_name, (SQLAlchemyAutoSchema,), {"Meta": Meta, **relations}
            )

            def fix_field(_, field_name, field_obj):
                if isinstance(field_obj, MarshEnum):
                    field_obj.by_value = True

            schema_class.on_bind_field = fix_field

            setattr(class_, "__marsh__", schema_class)

    return setup_schema_fn

event.listen(Mapper, "after_configured", setup_pydantic_models)

def model_init(model, args, kwargs):
    # Function that validates the model with pydantic before init
    model.__pydantic__(**kwargs)

class SQLAlchemyDatabase(Database):
    def __init__(self, db_path: str, models_folder: str, autogenerate_schemas: bool = False, add_user_tables: bool = False) -> None:
        super().__init__(db_path, add_user_tables)
        self.models_folder = models_folder
        self.engine = create_engine(f"sqlite:///{self.db_path}")

        self._sessionmaker = sessionmaker(bind=self.engine)
        self._nested_contexts = {}
        self._connections: Dict[int, Session] = {}

        if not os.path.exists(self.db_path):
            logger.info("Creating database from schema file.")
            self.create_database()

        if autogenerate_schemas:
            event.listen(Mapper, "after_configured", setup_marshmallow_schemas(self._sessionmaker()))

            for class_ in Base.__subclasses__():
                if hasattr(class_, "__tablename__"):
                    event.listen(class_, "init", model_init)

    def get_connection(self):
        session = self._sessionmaker()
        return session

    def create_database(self):
        if len(Base.metadata.tables) == 0:
            self._import_models()

        if not self.add_user_tables:
            Base.metadata.remove(User.__table__)
            Base.metadata.remove(AuthToken.__table__)

        Base.metadata.create_all(bind=self.engine)

    def _import_models(self):
        package = ".".join(self.models_folder.split(os.sep))
        for _, module_name, _ in pkgutil.walk_packages([self.models_folder], package + "."):
            importlib.import_module(module_name.replace("/", "."), __package__)

    def __enter__(self):
        thread_id = get_ident()
        nested_contexts = self._nested_contexts.get(thread_id, 0)
        nested_contexts += 1
        self._nested_contexts[thread_id] = nested_contexts

        if thread_id in self._connections:
            # Don't allow nested contexts.
            return self._connections[thread_id]

        self._connections[thread_id] = self.get_connection()
        return self._connections[thread_id]

    def __exit__(self, exc_type, exc_val, exc_tb):
        thread_id = get_ident()
        nested_contexts = self._nested_contexts.get(thread_id, 1)
        nested_contexts -= 1
        self._nested_contexts[thread_id] = nested_contexts

        if nested_contexts == 0:
            self._connections[thread_id].close()
            del self._connections[thread_id]
            del self._nested_contexts[thread_id]

    def create_user(self, user_id: str, username: str , hashed_pass: str):
        try:
            with self as session:
                session.add(User(id=user_id, name=username, password=hashed_pass))
                session.commit()
        except DatabaseError: # User already exists (unique constraint violated).
            return False
        return True

    def password_matches(self, username: str, password: str):
        with self as session:
            stmt = select(User.password).filter(User.name == username)
            try:
                result = session.execute(stmt).scalar_one()
            except Exception:
                return False

            return result == password

    def get_user_id(self, username: str):
        with self as session:
            stmt = select(User.id).filter(User.name == username)
            try:
               return session.execute(stmt).scalar_one()
            except Exception:
                return None

    def get_auth_token(self, user_id: str) -> str:
        with self as session:
            stmt = select(AuthToken.token).where(AuthToken.holder_id == user_id)
            return session.execute(stmt).scalar_one()

    def get_user_id_from_token(self, auth_token: str) -> Tuple[str, str] | None:
        with self as session:
            stmt = select(AuthToken.holder_id, User.name, AuthToken.expires).join(User).filter(AuthToken.token == auth_token)
            result = session.execute(stmt).first()

            if result is None:
                return None

            if result.t[2] < time():
                delete_stmt = delete(AuthToken).where(AuthToken.token == auth_token)
                session.execute(delete_stmt)
                session.commit()

                return None

            return (result.t[0], result.t[1])

    def save_auth_token(self, token: str, user_id: str, max_age: int):
        with self as session:
            expires = time() + max_age

            session.add(AuthToken(holder_id=user_id, token=token, expires=int(expires)))

            session.commit()

class SQLiteDatabase(Database):
    def __init__(self, db_path: str, schema_file: str | None = None, add_user_tables: bool = False, row_factory=None):
        super().__init__(db_path, add_user_tables)
        self.schema_file = schema_file
        self.add_user_tables = add_user_tables
        self._nested_contexts = {}
        self._connections = {}
        self._row_factory = row_factory

        if not os.path.exists(self.db_path):
            logger.info("Creating database from schema file.")
            self.create_database()

    def get_connection(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = self._row_factory
        return conn

    def create_user_tables(self, conn):
        conn.cursor().execute(
            """
            CREATE TABLE users (
                id NVARCHAR(32) PRIMARY KEY,
                name NVARCHAR(32) UNIQUE NOT NULL,
                password NVARCHAR(64) NOT NULL
            );
            """
        )
        conn.commit()

        conn.cursor().execute(
            """
            CREATE TABLE auth_tokens (
                holder_id NVARCHAR(32) NOT NULL,
                token NVARCHAR(64) PRIMARY KEY,
                expires INTEGER NOT NULL
            );
            """
        )
        conn.commit()

    def create_database(self):
        with self as session:
            if self.schema_file is not None:
                # Initialize database with provided schema, if it exists.
                with open(self.schema_file, "r") as f:
                    session.connection.cursor().executescript(f.read())
                session.connection.commit()

            if self.add_user_tables:
                self.create_user_tables(session.connection)

    def query(self, query: str, *params, format_func=None, default=None):
        return Query(query, *params, format_func=format_func, default=default, context=self)

    def execute_query(self, query: str, *params, commit=True) -> Cursor:
        """
        Execute a query against the SQLite database.

        :param query:   SQL query to be executed. Any '?' characters
                        will be replaced by the given parameters
        :param params:  Iterable of parameters to be added to the query
        :param commit:  Whether to commit the SQLite transaction after
                        the query has been executed
        """
        if self.connection is None:
            raise DBException("No database connection is active.")

        collected_params = []
        for param in params:
            if type(param) in (list, tuple):
                collected_params.append(tuple(param))
            else:
                collected_params.append(param)

        execute_many = collected_params != [] and all(isinstance(param, tuple) for param in collected_params)

        if not execute_many:
            collected_params = tuple(collected_params)

        try:
            if execute_many:
                cursor = self.connection.cursor().executemany(query, collected_params)
            else:
                cursor = self.connection.cursor().execute(query, collected_params)

            if commit:
                self.connection.commit()

            return cursor

        except (OperationalError, ProgrammingError, DatabaseError) as exc:
            raise DBException(exc.args)

    def __enter__(self):
        thread_id = get_ident()
        nested_contexts = self._nested_contexts.get(thread_id, 0)
        nested_contexts += 1
        self._nested_contexts[thread_id] = nested_contexts

        if thread_id in self._connections:
            # Don't allow nested contexts.
            return self

        self._connections[thread_id] = self.get_connection()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        thread_id = get_ident()
        nested_contexts = self._nested_contexts.get(thread_id, 1)
        nested_contexts -= 1
        self._nested_contexts[thread_id] = nested_contexts

        if nested_contexts == 0:
            self._connections[thread_id].close()
            del self._connections[thread_id]
            del self._nested_contexts[thread_id]

    def create_user(self, user_id, username, hashed_pass):
        try:
            with self as session:
                query = "INSERT INTO users(id, name, password) VALUES (?, ?, ?)"
                session.execute_query(query, user_id, username, hashed_pass)
        except DatabaseError: # User already exists (unique constraint violated).
            return False
        return True

    def password_matches(self, username, password):
        with self as session:
            query = "SELECT password FROM users WHERE name=?"
            result = session.execute_query(query, username).fetchone()
            if result is None:
                return False

            return result[0] == password

    def get_user_id(self, username):
        with self as session:
            query = "SELECT id FROM users WHERE name=?"
            result = session.execute_query(query, username).fetchone()
            return result if result is None else result[0]

    def get_user_id_from_token(self, auth_token: str) -> Tuple[str, str] | None:
        with self as session:
            query = """
                SELECT
                    at.holder_id,
                    u.name,
                    at.expires
                FROM auth_tokens AS at
                INNER JOIN users AS u
                    ON u.id = at.holder_id
                WHERE at.token = ?
            """
            result = session.execute_query(query, auth_token).fetchone()

            if result is None:
                return None

            if result[2] < time():
                # Token is expired
                query_delete = "DELETE FROM auth_tokens WHERE token = ?"
                session.execute_query(query_delete, auth_token)
                return None

            return result

    def save_auth_token(self, token: str, user_id: str, max_age: int):
        with self as session:
            expires = time() + max_age
            query = "INSERT INTO auth_tokens(holder_id, token, expires) VALUES (?, ?, ?)"
            session.execute_query(query, user_id, token, int(expires))
