from __future__ import annotations

import sqlite3
from collections import namedtuple
from typing import Type, Dict, Union, Any

from .db import AbstractDbHandler
from .query import InsertQueryBuilder, UpdateQueryBuilder, DeleteQueryBuilder, SelectQueryBuilder


DbOperationResult = namedtuple('DbOperationResult', ['success', 'error'], defaults=(False, None))


class _ICrud(object):

    def delete(self) -> DbOperationResult:
        raise NotImplementedError()

    def refresh(self) -> DbOperationResult:
        raise NotImplementedError()

    def save(self, update_existing: bool = True) -> DbOperationResult:
        raise NotImplementedError()


class AbstractModel(dict, _ICrud):

    _db_handler: AbstractDbHandler = None
    _table_name: str = None
    _id_col_name: str = None

    @classmethod
    def from_dict(cls, a_dict: dict):
        model = cls()
        model.update(a_dict)
        return model

    @classmethod
    def create_group(cls):
        return ModelList(cls)

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

    @property
    def table_name(self):
        return self._table_name

    def delete(self) -> DbOperationResult:
        pass

    def refresh(self) -> DbOperationResult:
        pass

    def save(self, update_existing: bool = True) -> DbOperationResult:
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


class ModelList(list, _ICrud):

    _model_type: Type[AbstractModel] = None
    _index: Dict[Union[str, int, None], Any] = None

    def __init__(self, model_type: Type[AbstractModel]) -> None:
        super().__init__()
        if not isinstance(model_type, type) or not issubclass(model_type, AbstractModel):
            raise TypeError("Invalid model type provided.")
        self._model_type = model_type

    def append(self, __object: _model_type) -> None:
        if not isinstance(__object, self._model_type):
            raise TypeError("Incorrect model type.")
        else:
            super().append(__object)

    def insert(self, __index: int, __object: _model_type) -> None:
        if not isinstance(__object, self._model_type):
            raise TypeError("Incorrect model type.")
        else:
            super().insert(__index, __object)

    def __add__(self, x: ModelList[_model_type]):
        if not isinstance(x, ModelList) or x._model_type is not self._model_type:
            raise TypeError("Incorrect model type.")
        else:
            return super().__add__(x)

    def __iadd__(self, x: ModelList[_model_type]):
        if not isinstance(x, ModelList) or x._model_type is not self._model_type:
            raise TypeError("Incorrect model type.")
        else:
            return super().__iadd__(x)

    def _index_models(self):
        self._index = {None: []}
        for model in self:
            if model.id is None:
                self._index[None].append(model)
            else:
                self._index[model.id] = model

    def delete(self) -> DbOperationResult:
        if len(self) == 0:
            return DbOperationResult(False, "List is empty. No operation to perform.")
        pass

    def refresh(self) -> DbOperationResult:
        if len(self) == 0:
            return DbOperationResult(False, "List is empty. No operation to perform.")
        q = SelectQueryBuilder(self[0].table_name)

    def save(self, update_existing: bool = True) -> DbOperationResult:
        if len(self) == 0:
            return DbOperationResult(False, "List is empty. No operation to perform.")
        pass
