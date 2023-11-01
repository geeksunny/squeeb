from __future__ import annotations

import logging
import sqlite3
from abc import ABCMeta, abstractmethod
from contextlib import closing
from dataclasses import dataclass
from typing import Any, List, Tuple, Dict, TypeVar

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
    _conn = None

    def __init__(self) -> None:
        super().__init__()
        self._conn = sqlite3.connect(self._db_filename())
        self._conn.row_factory = sqlite3.Row

    @abstractmethod
    def _db_filename(self) -> str:
        pass

    @abstractmethod
    def _init_tables(self) -> bool:
        pass

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
            logger.error('QUERY BUILD ERROR: %s', query['error'])
            return DbHandlerNoResult(error=query['error'])
        return self._exec_raw_query_no_result(query['query'], query['args'])

    def exec_query_single_result(self, query_builder: QueryBuilder) -> DbHandlerSingleResult:
        query = query_builder.build()
        if 'error' in query:
            logger.error('QUERY BUILD ERROR: %s', query['error'])
            return DbHandlerSingleResult(error=query['error'])
        return self._exec_raw_query_single_result(query['query'], query['args'])

    def exec_query_all_results(self, query_builder: QueryBuilder) -> DbHandlerMultiResult:
        query = query_builder.build()
        if 'error' in query:
            logger.error('QUERY BUILD ERROR: %s', query['error'])
            return DbHandlerMultiResult(error=query['error'])
        return self._exec_raw_query_all_results(query['query'], query['args'])

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


DbHandler = TypeVar('DbHandler', bound=AbstractDbHandler)


__db_handlers: Dict[str, DbHandler] = {}


def register_db_handler(db_handler: DbHandler, name: str = 'default'):
    __db_handlers[name] = db_handler


def _get_db_handler(name: str = 'default') -> DbHandler:
    return __db_handlers[name] if name in __db_handlers else None
