from __future__ import annotations

import sqlite3
from abc import ABCMeta, abstractmethod
from copy import deepcopy
from dataclasses import dataclass, field
from types import MappingProxyType as FrozenDict
from typing import Type, Dict, TypeVar, List, ClassVar

from squeeb.db import DbHandler, _get_db_handler, AbstractDbHandler
from squeeb.query import InsertQueryBuilder, UpdateQueryBuilder, DeleteQueryBuilder, SelectQueryBuilder, where
from .columns import TableColumn, PrimaryKey
from squeeb.util import camel_to_snake_case
from squeeb.common import ValueMapping
from squeeb.query.queries import CreateTableQueryBuilder


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


class ModelMetaClass(ABCMeta):

    def __new__(metacls, cls, bases, classdict, **kwargs):
        mapping = {}
        for name, value in classdict.items():
            if isinstance(value, TableColumn):
                mapping[name] = value.column_name if value.column_name is not None else name
        result_class = super().__new__(metacls, cls, bases, classdict, **kwargs)
        result_class.__mapping__ = mapping
        return result_class

    def __setattr__(self, __name, __value):
        if __name is '__mapping__':
            if isinstance(__value, Dict):
                """When setting the column mapping, if a mapping already exists then the new mapping will be combined
                with the old mapping. This ensures extended models will have a mapping for all inherited fields.
                The mapping will be stored as a MappingProxy to ensure a read-only status. MappingProxyType is imported
                as FrozenDict to convey intended use case."""
                __value = FrozenDict(getattr(self, __name) | __value) if hasattr(self, __name) else FrozenDict(__value)
                setattr(self, '__mapping_inverse__', FrozenDict(dict(map(reversed, __value.items()))))
            else:
                raise TypeError("Column mapping must be a dictionary.")
        elif __name is '__mapping_inverse__':
            if not isinstance(__value, FrozenDict):
                raise TypeError("Inverse column mapping must already be a frozen dictionary.")
        super().__setattr__(__name, __value)


class AbstractModel(_ICrud, metaclass=ModelMetaClass):
    # _changed_fields: List[str]

    __mapping__: ClassVar[Dict[str, str]]
    __mapping_inverse__: ClassVar[Dict[str, str]]
    _db_handler: ClassVar[AbstractDbHandler]
    _table_name: ClassVar[str]
    _initialized: ClassVar[bool]

    @classmethod
    def create_group(cls):
        return ModelList(cls)

    def __init__(self) -> None:
        self._changed_fields = set()

    def __new__(cls, *more):
        """Copies new instances of the model's default column objects."""
        instance = super().__new__(cls)
        for name in instance.__mapping__:
            instance.__dict__[name] = deepcopy(getattr(instance, name))
            """Check for a column with the PrimaryKey constraint defined."""
            if instance.__dict__[name].constraints is not None:
                for constraint in instance.__dict__[name].constraints:
                    if isinstance(constraint, PrimaryKey):
                        instance.__dict__['_id'] = instance.__dict__[name]
                        instance.__dict__['_id_col_name'] = instance.__dict__[name].column_name \
                            if instance.__dict__[name].column_name is not None else name
        # TODO: Refactor in a way to accommodate tables relying on sqlite's built-in `rowid` value
        #  in lieu of a primary key
        if not hasattr(instance, '_id'):
            raise TypeError("Model class does not have a column defined as Primary Key.")
        return instance

    def __setattr__(self, __name, __value):
        """Setting a new value on a TableColumn field will set the value directly to the column rather than reassign
        the column reference."""
        if hasattr(self, __name) and isinstance(attr := getattr(self, __name), TableColumn):
            attr.value = __value
            self._changed_fields.add(attr)
        else:
            super().__setattr__(__name, __value)

    @classmethod
    def _create_table_query(cls) -> CreateTableQueryBuilder:
        # TODO: Implement the rest of this query builder once the class is completed.
        return CreateTableQueryBuilder(cls._table_name)

    @property
    def db_handler(self) -> DbHandler:
        return self._db_handler

    @property
    def id(self):
        return getattr(self, '_id')

    @property
    def id_col_name(self):
        return getattr(self, '_id_col_name')

    @property
    def table_name(self) -> str:
        return self._table_name

    @classmethod
    def _init_table(self):
        q = self._create_table_query()
        result = self._db_handler.exec_query_no_result(q)
        if isinstance(result, sqlite3.Error):
            return DbOperationResult(error=DbOperationError(result))
        elif isinstance(result, int):
            self.__class__._initialized = True
            return DbOperationResult()
        else:
            return DbOperationResult(error=DbOperationError("Unknown error encountered."))

    def delete(self) -> DbOperationResult:
        # TODO: Review and confirm if this still works after AbstractModel class refactor.
        if self.id is None:
            return DbOperationResult("Model is not saved and cannot be deleted. No action took place.")
        else:
            q = DeleteQueryBuilder(self._table_name)
            q.where = where(self.id_col_name).equals(self.id.value)
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
        # TODO: Review and confirm if this still works after AbstractModel class refactor.
        if self.id.value is None:
            q = InsertQueryBuilder(self.table_name).set_value(self._get_value_map())
            action = 'Insert'
            # TODO: Retrieve and update the self.id value after insertion.
        else:
            q = UpdateQueryBuilder(self.table_name).set_value(self._get_value_map(True))
            q.where = where(self.id_col_name).equals(self.id.value)
            action = 'Update'
        result = self._db_handler.exec_query_single_result(q)
        return DbOperationResult('%s operation %s' % (action, 'success' if result.success else 'failure'),
                                 [result.row], DbOperationError(result.error) if result.error is not None else None)

    def _get_value_map(self, only_updated_fields: bool = False) -> List[ValueMapping]:
        value_map = []
        for class_field_name, column_name in self.__mapping__.items():
            attr = getattr(self, class_field_name)
            if not only_updated_fields or attr not in self._changed_fields:
                continue
            value_map.append({column_name: attr.value})
        return value_map

    def populate(self, columns_and_values: ValueMapping) -> None:
        for column_name, value in columns_and_values.items():
            if column_name not in self.__mapping_inverse__:
                raise AttributeError(f"No column mapping exists for column {column_name}.")
            getattr(self, self.__mapping_inverse__[column_name]).value = value

    def from_sqlite(self, row: sqlite3.Row) -> None:
        # TODO: Test that this works the same as feeding a dict into self.populate;
        #  Remove this method and add sqlite3.Row to the type hint of self.populate if so.
        self.populate(row)
        # for sql_key in row.keys():
        #     key = sqlite_field_mapping[sql_key]\
        #         if sqlite_field_mapping is not None and sql_key in sqlite_field_mapping\
        #         else sql_key
        #     self[key] = row[sql_key]


ModelType = TypeVar('ModelType', bound=AbstractModel)


def table(cls: Type[AbstractModel] = None, db_handler_name: str = 'default', table_name: str = None):
    """
    Decorates an AbstractModel subclass to wire up internal dependencies.
    :param cls: The class being generated. This is passed automatically and can be ignored.
    :param db_handler_name: The name of a DbHandler that's been registered with `register_db_handler(db_handler, name)`.
           This uses the default handler if no name is passed.
    :param table_name: The table name that this model will be represented as in the database.
           The name will default to an all lower-case snake-cased pluralized version of your models name.
           For example: A model class named 'ItemRecord' will become 'item_records'.
    :return: A wrapped subclass of your decorated class definition.
    """
    if cls is not None and not issubclass(cls, AbstractModel):
        raise TypeError("Decorated class must be a subclass of AbstractModel.")
    if not isinstance(db_handler_name, str):
        raise TypeError("Database handler name must be a string value.")
    if table_name is not None and (not isinstance(table_name, str) or len(table_name) == 0):
        raise TypeError("Invalid table_name provided.")

    def wrap(clss):
        _table_name = table_name if table_name is not None else f'{camel_to_snake_case(clss.__name__, lowercase=True)}s'

        class TableClass(clss):

            def __init__(self) -> None:
                if not hasattr(self.__class__, '_db_handler') or self.__class__._db_handler is None:
                    self.__class__._db_handler = _get_db_handler()
                    if self.__class__._db_handler is None:
                        raise ValueError("Database handler for this model has not been registered.")
                if not hasattr(self.__class__, '_initialized') or self.__class__._initialized is not True:
                    init_result = self.__class__._init_table()
                    if not init_result.success:
                        raise RuntimeError("The database table failed to be created.")
                super().__init__()

        TableClass.__name__ = TableClass.__qualname__ = clss.__name__
        TableClass._table_name = _table_name
        return TableClass

    return wrap if cls is None else wrap(cls)


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
