from __future__ import annotations

import sqlite3
from abc import ABCMeta, abstractmethod
from dataclasses import dataclass, field, InitVar
from typing import Type, Dict, Any, TypeVar, Generator, Set

from .db import AbstractDbHandler
from .query import InsertQueryBuilder, UpdateQueryBuilder, DeleteQueryBuilder, SelectQueryBuilder, where


class DbOperationError(Exception):
    pass


@dataclass(frozen=True)
class DbOperationResult:
    error: DbOperationError | str = None

    @property
    def success(self):
        return self.error is not None


class _ICrud(object, metaclass=ABCMeta):

    @abstractmethod
    def delete(self) -> DbOperationResult:
        pass

    @abstractmethod
    def refresh(self) -> DbOperationResult:
        pass

    @abstractmethod
    def save(self, update_existing: bool = True) -> DbOperationResult:
        pass


class AbstractModel(dict, _ICrud, metaclass=ABCMeta):
    # _db_handler: AbstractDbHandler
    # _table_name: str
    # _id_col_name: str
    # _col_names: Set[str]
    # _changed_fields: List[str]

    @classmethod
    def from_dict(cls, a_dict: dict):
        model = cls()
        for (k, v) in a_dict.items():
            model[k] = v
        return model

    @classmethod
    def create_group(cls):
        return ModelList(cls)

    def __init__(self, db_handler: AbstractDbHandler, table_name: str, column_names: Set[str], id_col_name: str = "id") -> None:
        super().__init__()
        if not isinstance(db_handler, AbstractDbHandler):
            raise TypeError("Invalid DB Handler for this model class.")
        self._db_handler = db_handler
        if not isinstance(table_name, str) or len(table_name) == 0:
            raise TypeError("Invalid table_name provided.")
        self._table_name = table_name
        if isinstance(column_names, set) and len(column_names) > 0:
            for col_name in column_names:
                if not isinstance(col_name, str):
                    raise TypeError("Column names must be strings.")
        else:
            raise TypeError("List of one or more column names required.")
        self._col_names = set(column_names)
        if not isinstance(id_col_name, str) or len(id_col_name) == 0:
            raise TypeError("Invalid id_col_name provided.")
        self._id_col_name = id_col_name
        self._changed_fields = set()

    def __setitem__(self, k: str, v: Any) -> None:
        print('changing %s to %s' % (k, str(v)))
        if k in self._col_names:
            self._changed_fields.add(k)
            super().__setitem__(k, v)
        else:
            raise KeyError("Model does not contain column with name '%s'" % k)

    @property
    def db_handler(self):
        return self._db_handler

    @property
    def id(self):
        return self[self._id_col_name] if self._id_col_name in self else None

    @property
    def table_name(self):
        return self._table_name

    @property
    def id_col_name(self):
        return self._id_col_name

    def delete(self) -> DbOperationResult:
        pass
        if self.id is None:
            return DbOperationResult("Model is not saved and cannot be deleted. No action took place.")
        else:
            q = DeleteQueryBuilder(self._table_name)
            q.where = where(self._id_col_name).equals(self.id)
            result = self._db_handler.exec_query_no_result(q)
            if isinstance(result, sqlite3.Error):
                return DbOperationResult(error=DbOperationError(result))
            elif isinstance(result, int):
                return DbOperationResult("Rows changed: %d" % result)
            else:
                return DbOperationResult(error=DbOperationError("Unknown error encountered."))

    def refresh(self) -> DbOperationResult:
        pass

    def save(self, update_existing: bool = True) -> DbOperationResult:
        if self.id is None:
            q = InsertQueryBuilder(self._table_name).set_value(self)
            action = 'Insert'
        else:
            q = UpdateQueryBuilder(self._table_name).set_value(self)
            q.where = where(self._id_col_name).equals(self.id)
            action = 'Update'
        result = self._db_handler.exec_query_single_result(q)
        return DbOperationResult('%s operation %s' % (action, 'success' if result.success else 'failure'),
                                 [result.row], DbOperationError(result.error) if result.error is not None else None)

    def _set_if_tag_exists(self, field_name, source, source_field=None) -> None:
        if source_field is None:
            source_field = field_name
        if source[source_field] is not None:
            self[field_name] = source[source_field][0] if isinstance(source[source_field], list)\
                else source[source_field]

    @abstractmethod
    def populate(self, taglib_song) -> None:
        pass

    def from_sqlite(self, row: sqlite3.Row, sqlite_field_mapping=None) -> None:
        for sql_key in row.keys():
            key = sqlite_field_mapping[sql_key]\
                if sqlite_field_mapping is not None and sql_key in sqlite_field_mapping\
                else sql_key
            self[key] = row[sql_key]


ModelType = TypeVar('ModelType', bound=AbstractModel)


@dataclass(frozen=True)
class DbOperationResults(DbOperationResult):
    results: Dict[ModelType, DbOperationResult] = field(default_factory=dict)


class ModelList(list, _ICrud):
    # _model_type: Type[ModelType]
    # _index: Dict[Union[str, int, None], Any]

    def __init__(self, model_type: Type[ModelType]) -> None:
        super().__init__()
        if not isinstance(model_type, type) or not issubclass(model_type, AbstractModel):
            raise TypeError("Invalid model type provided.")
        self._model_type = model_type

    def append(self, __object: ModelType) -> None:
        if not isinstance(__object, self._model_type):
            raise TypeError("Incorrect model type.")
        else:
            super().append(__object)

    def insert(self, __index: int, __object: ModelType) -> None:
        if not isinstance(__object, self._model_type):
            raise TypeError("Incorrect model type.")
        else:
            super().insert(__index, __object)

    def __add__(self, x: ModelList[ModelType]):
        if not isinstance(x, ModelList) or x._model_type is not self._model_type:
            raise TypeError("Incorrect model type.")
        else:
            return super().__add__(x)

    def __iadd__(self, x: ModelList[ModelType]):
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

    def delete(self) -> DbOperationResults:
        if len(self) == 0:
            return DbOperationResult("List is empty. No operation to perform.")
        ids = []
        cant_update = []
        for model in self:
            if model is not None:
                if model.id is not None:
                    ids.append(model.id)
                else:
                    cant_update.append(model)
        if len(ids) > 0:
            q = DeleteQueryBuilder(self[0].table_name)
            q.where = where(self[0].id_col_name).is_in(ids)
            result = self[0].db_handler.exec_query_no_result(q)
            msg = '%d of %d rows deleted. %d skipped.' % (result.rowcount, len(ids), len(cant_update) > 0)
            return DbOperationResult(msg, error=result.error, row_count=result.rowcount)
        else:
            return DbOperationResult('%d of %d rows skipped.' % (len(cant_update), len(self)))

    def refresh(self) -> DbOperationResults:
        if len(self) == 0:
            return DbOperationResult("List is empty. No operation to perform.")
        self._index_models()
        ids = list(filter(lambda k: k is not None, self._index.keys()))
        q = SelectQueryBuilder(self[0].table_name)
        q.where = where(self[0].id_col_name).is_in(ids)
        self[0].db_handler.exec_query_all_results(q)

    def save(self, update_existing: bool = True) -> DbOperationResults:
        if len(self) == 0:
            return [DbOperationResult("List is empty. No operation to perform.")]
        save = []
        update = []
        results = []
        for model in self:
            if model.id is None:
                save.append(model)
            else:
                update.append(model)
        if len(save) > 0:
            pass
        if len(update) > 0:
            pass
        for model in save:
            pass
        for model in update:
            pass
        return results

    def _save_many(self):
        pass

    def _update_many(self):
        pass
