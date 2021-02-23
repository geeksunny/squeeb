from __future__ import annotations

import logging
import sqlite3
from contextlib import closing
from typing import Any, List, Tuple, Union


logger = logging.getLogger()


class AbstractDbHandler(object):

    _conn = None

    def __init__(self) -> None:
        super().__init__()
        self._conn = sqlite3.connect(self._db_filename())
        self._conn.row_factory = sqlite3.Row

    def _db_filename(self) -> str:
        raise NotImplementedError()

    def _init_tables(self) -> bool:
        raise NotImplementedError()

    def __del__(self):
        self.close()

    def _exec_raw_query_no_result(self, query_str: str, args=None) -> bool:
        try:
            with closing(self._conn.cursor()) as c:
                c.execute(query_str, args)
                return True
        except sqlite3.Error as e:
            logger.error(e)
            return False

    def _exec_raw_query_single_result(self, query_str: str, args=None) -> sqlite3.Row:
        try:
            with closing(self._conn.cursor()) as c:
                c.execute(query_str, args)
                return c.fetchone()
        except sqlite3.Error as e:
            logger.error(e)
            return None

    def _exec_raw_query_all_results(self, query_str: str, args: Tuple[Any] = None) -> Union[List[sqlite3.Row], None]:
        try:
            with closing(self._conn.cursor()) as c:
                c.execute(query_str, args)
                return c.fetchall()
        except sqlite3.Error as e:
            logger.error(e)
            return None

    def exec_query_no_result(self, query_builder) -> bool:
        query = query_builder.build()
        if 'error' in query:
            logger.error('QUERY BUILD ERROR: %s', query['error'])
            return False
        return self._exec_raw_query_no_result(query['query'], query['args'])

    def exec_query_single_result(self, query_builder) -> Union[sqlite3.Row, None]:
        query = query_builder.build()
        if 'error' in query:
            logger.error('QUERY BUILD ERROR: %s', query['error'])
            return None
        return self._exec_raw_query_single_result(query['query'], query['args'])

    def exec_query_all_results(self, query_builder) -> Union[List[sqlite3.Row], None]:
        query = query_builder.build()
        if 'error' in query:
            logger.error('QUERY BUILD ERROR: %s', query['error'])
            return None
        return self._exec_raw_query_all_results(query['query'], query['args'])

    def _table_exists(self, table_name) -> bool:
        return self._exec_raw_query_single_result(
            "SELECT * FROM sqlite_master WHERE type='table' AND name='%s'" % table_name)[0] == 1

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None
