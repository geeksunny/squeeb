from __future__ import annotations

import logging
import sqlite3
from contextlib import closing
from dataclasses import dataclass
from typing import List, Type, Tuple, Any, ClassVar

from .model.models import _sort_models, Model, _validate_foreign_keys
from .query.queries import QueryBuilder, SelectQueryBuilder
from .util import Singleton, camel_to_snake_case


class BaseDbHandlerResult:
    error: sqlite3.Error = None

    @property
    def success(self):
        return self.error is not None


@dataclass(frozen=True)
class DbHandlerNoResult(BaseDbHandlerResult):
    rowcount: int = 0
    error: sqlite3.Error = None


@dataclass(frozen=True)
class DbHandlerSingleResult(BaseDbHandlerResult):
    row: sqlite3.Row = None
    error: sqlite3.Error = None


@dataclass(frozen=True)
class DbHandlerMultiResult(BaseDbHandlerResult):
    rows: List[sqlite3.Row] = None
    error: sqlite3.Error = None

    @property
    def rowcount(self):
        return len(self.rows) if self.rows is not None else 0


logger = logging.getLogger()


class Database(metaclass=Singleton):
    # _conn = None
    __tables__: ClassVar[List[Type[Model]]] = []

    def __init__(self, file_path: str = None, version: int = 0):
        if file_path is None:
            file_path = f'{self.__class__.__name__}.db'
        # Open database or create if not exists.
        self._conn = sqlite3.connect(file_path)
        self._conn.row_factory = sqlite3.Row
        # Create tables if they do not exist.
        self._init_tables()

    def __del__(self):
        self.close()

    @classmethod
    def register_table(cls, table_model: Type[Model]):
        cls.__tables__.append(table_model) if table_model not in cls.__tables__ else None

    def _init_tables(self):
        _validate_foreign_keys(self)
        models = _sort_models(self.__tables__)
        for model in models:
            success = model.init_table()
            if not success:
                return False
        return True

    def _exec_raw_query_no_result(self, query_str: str, args: Any = None) -> DbHandlerNoResult:
        try:
            with closing(self._conn.cursor()) as c:
                c.execute(query_str, args if args is not None else ())
                return DbHandlerNoResult(c.rowcount)
        except sqlite3.Error as e:
            logger.error(e)
            return DbHandlerNoResult(error=e)

    def _exec_raw_query_single_result(self, query_str: str, args: Any = None) -> DbHandlerSingleResult:
        try:
            with closing(self._conn.cursor()) as c:
                c.execute(query_str, args if args is not None else ())
                return DbHandlerSingleResult(c.fetchone())
        except sqlite3.Error as e:
            logger.error(e)
            return DbHandlerSingleResult(error=e)

    def _exec_raw_query_all_results(self, query_str: str, args: Tuple[Any] = None) -> DbHandlerMultiResult:
        try:
            with closing(self._conn.cursor()) as c:
                c.execute(query_str, args if args is not None else ())
                return DbHandlerMultiResult(c.fetchall())
        except sqlite3.Error as e:
            logger.error(e)
            return DbHandlerMultiResult(error=e)

    def exec_query_no_result(self, query_builder: QueryBuilder) -> DbHandlerNoResult:
        query = query_builder.build()
        if 'error' in query:
            logger.error('QUERY BUILD ERROR: %s', query.error)
            return DbHandlerNoResult(error=query.error)
        return self._exec_raw_query_no_result(query.query, query.args)

    def exec_query_single_result(self, query_builder: QueryBuilder) -> DbHandlerSingleResult:
        query = query_builder.build()
        if 'error' in query:
            logger.error('QUERY BUILD ERROR: %s', query.error)
            return DbHandlerSingleResult(error=query.error)
        return self._exec_raw_query_single_result(query.query, query.args)

    def exec_query_all_results(self, query_builder: QueryBuilder) -> DbHandlerMultiResult:
        query = query_builder.build()
        if 'error' in query:
            logger.error('QUERY BUILD ERROR: %s', query.error)
            return DbHandlerMultiResult(error=query.error)
        return self._exec_raw_query_all_results(query.query, query.args)

    def exec_many_no_result(self, query_builder: QueryBuilder) -> List[DbHandlerNoResult]:
        pass

    def exec_many_single_result(self, query_builder: QueryBuilder) -> List[DbHandlerSingleResult]:
        pass

    def exec_many_all_results(self, query_builder: QueryBuilder) -> List[DbHandlerMultiResult]:
        pass

    def _table_exists(self, table_name) -> bool:
        return self._exec_raw_query_single_result(
            "SELECT * FROM sqlite_master WHERE type='table' AND name='%s'" % table_name)[0] == 1

    def _get_user_version(self) -> int:
        result = self._exec_raw_query_single_result("PRAGMA user_version")
        return result.row['user_version']

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None


class DatabaseManager(Singleton):

    def __new__(metacls, name, bases, namespace, **kwargs):
        try:
            db = kwargs['database']
            if not issubclass(db, Database):
                raise TypeError('Class database argument must be a subclass of Database.')
        except KeyError:
            raise TypeError('Class definition is missing the `database` argument.')

        def make_get_all_method(model_class: Type[Model]):
            def get_all(self):
                query = SelectQueryBuilder(model_class.__table_name__).build()
                result = self._db._exec_raw_query_all_results(query.query)
                # TODO: Create array of objects deserialized from result.rows, return that instead.
                return result.rows
            return get_all

        cls = super().__new__(metacls, name, bases, namespace)
        setattr(cls, '_db', db())
        for table in db.__tables__:
            setattr(cls, f'get_all_{camel_to_snake_case(table.__name__, lowercase=True)}s', make_get_all_method(table))
        return cls


def make_database_class(name: str, file_path: str = None, version: int = 0):
    class Db(Database):
        def __init__(self):
            super().__init__(file_path, version)

    Db.__name__ = Db.__qualname__ = name
    return Db
