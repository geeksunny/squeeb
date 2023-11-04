from __future__ import annotations

import logging
import sqlite3
from abc import ABCMeta
from contextlib import closing
from dataclasses import dataclass
from typing import List, TypeVar, Type, Tuple, Any, ClassVar

from .manager import _get_table_models
from .model.models import sort_models
from .query.queries import QueryBuilder


class BaseDbHandlerResult:
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


class AbstractDbHandler(object, metaclass=ABCMeta):
    # _conn = None

    _db_filename: ClassVar[str]
    _name: ClassVar[str]

    def __init__(self) -> None:
        super().__init__()
        self._conn = sqlite3.connect(self._db_filename)
        self._conn.row_factory = sqlite3.Row

    def _init_tables(self) -> bool:
        models = sort_models(_get_table_models(self.__class__._name))
        for model in models:
            success = model.init_table_if_needed()
            if not success:
                return False
        return True

    def __del__(self):
        self.close()

    def _exec_raw_query_no_result(self, query_str: str, args=None) -> DbHandlerNoResult:
        try:
            with closing(self._conn.cursor()) as c:
                c.execute(query_str, args)
                return DbHandlerNoResult(c.rowcount)
        except sqlite3.Error as e:
            logger.error(e)
            return DbHandlerNoResult(error=e)

    def _exec_raw_query_single_result(self, query_str: str, args=None) -> DbHandlerSingleResult:
        try:
            with closing(self._conn.cursor()) as c:
                c.execute(query_str, args)
                return DbHandlerSingleResult(c.fetchone())
        except sqlite3.Error as e:
            logger.error(e)
            return DbHandlerSingleResult(error=e)

    def _exec_raw_query_all_results(self, query_str: str, args: Tuple[Any] = None) -> DbHandlerMultiResult:
        try:
            with closing(self._conn.cursor()) as c:
                c.execute(query_str, args)
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

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None


def database(cls: Type[AbstractDbHandler] = None, name: str = 'default', filename: str = None):
    """
    Decorates an AbstractDbHandler subclass to wire up internal dependencies.
    :param cls: The class being generated. This is passed automatically and can be ignored.
    :param name: The name that this database will be associated with. Models that will be stored in this database should
           reference this name in their decorators. Default name is 'default'.
    :param filename: The filename that will be used for the sqlite database.
    :return: A wrapped subclass of your decorated class definition.
    """
    if cls is not None and not issubclass(cls, AbstractDbHandler):
        raise TypeError("Decorated class must be a subclass of AbstractDbHandler.")
    if not isinstance(name, str) or len(name) == 0:
        raise TypeError("Database name must be a string value.")
    if filename is None:
        filename = f'{name}.db'

    def wrap(clss):
        class Database(clss):
            pass

        Database.__name__ = Database.__qualname__ = clss.__name__
        Database._db_filename = filename
        return Database

    return wrap if cls is None else wrap(cls)


DbHandler = TypeVar('DbHandler', bound=AbstractDbHandler)
