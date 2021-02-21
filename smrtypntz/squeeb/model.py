import sqlite3

from .db import AbstractDbHandler


class AbstractModel(dict):

    _db_handler: AbstractDbHandler = None
    _table_name: str = None
    _id_col_name: str = None

    @classmethod
    def from_dict(cls, a_dict: dict):
        model = cls()
        model.update(a_dict)
        return model

    def __init__(self, db_handler: AbstractDbHandler, table_name: str, id_col_name: str = "id") -> None:
        super().__init__()
        if not isinstance(db_handler, AbstractDbHandler):
            raise TypeError("Invalid DB Handler for this model class.")
        self._db_handler = db_handler
        if not isinstance(table_name, str) or len(table_name) == 0:
            raise TypeError("Invalid table_name provided.")
        self._table_name = table_name
        if not isinstance(id_col_name, str) or len(id_col_name) == 0:
            raise TypeError("Invalid id_col_name provided.")
        self._id_col_name = id_col_name

    @property
    def id(self):
        return self[self._id_col_name] if self._id_col_name in self else None

    def delete(self):
        pass

    def refresh(self):
        pass

    def save(self):
        pass

    def _set_if_tag_exists(self, field_name, source, source_field=None) -> None:
        if source_field is None:
            source_field = field_name
        if source[source_field] is not None:
            self[field_name] = source[source_field][0] if isinstance(source[source_field], list)\
                else source[source_field]

    def populate(self, taglib_song) -> None:
        raise NotImplementedError()

    def from_sqlite(self, row: sqlite3.Row, sqlite_field_mapping=None) -> None:
        for sql_key in row.keys():
            key = sqlite_field_mapping[sql_key]\
                if sqlite_field_mapping is not None and sql_key in sqlite_field_mapping\
                else sql_key
            self[key] = row[sql_key]
