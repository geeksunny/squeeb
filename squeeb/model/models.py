from __future__ import annotations

import functools
import sqlite3
from abc import ABCMeta, abstractmethod
from copy import deepcopy
from dataclasses import dataclass, field
from types import MappingProxyType as FrozenDict
from typing import Type, Dict, List, ClassVar, TYPE_CHECKING

from squeeb.common import ValueMapping
from squeeb.query import InsertQueryBuilder, UpdateQueryBuilder, DeleteQueryBuilder, SelectQueryBuilder, where
from squeeb.query.queries import CreateTableQueryBuilder
from squeeb.util import FrozenList
from .columns import TableColumn, PrimaryKey, ForeignKey, ColumnConstraint, copy_column
from .index import TableIndex

if TYPE_CHECKING:
    from squeeb.db import Database


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


class ModelNamespace(dict):
    """
    Model Namespace class to handle setting default column names at model class definition.
    """

    def __setitem__(self, __key, __value):
        if isinstance(__value, TableColumn):
            if not hasattr(__value, '__column_name__'):
                __value = copy_column(__value, __key)
        super().__setitem__(__key, __value)


class ModelMetaClass(ABCMeta):

    @classmethod
    def __prepare__(metacls, __name, __bases, **kwds):
        return ModelNamespace(**kwds)

    def __new__(metacls, cls, bases, classdict, **kwargs):
        result_class = super().__new__(metacls, cls, bases, classdict, **kwargs)
        mapping = {}
        indexes = []
        for name, value in classdict.items():
            if isinstance(value, TableColumn):
                mapping[name] = value.column_name
                """Check for a column with the PrimaryKey constraint defined."""
                if value.constraint is not None and isinstance(value.constraint, PrimaryKey):
                    setattr(result_class, '__id_key__', name)
                    setattr(result_class, '_id_col_name', value.column_name)
            elif isinstance(value, TableIndex):
                value._setup(result_class, name)
                indexes.append(value)
        result_class.__mapping__ = mapping
        result_class.__indexes__ = indexes
        # TODO: Refactor in a way to accommodate tables relying on sqlite's built-in `rowid` value
        #  in lieu of a primary key
        if len(mapping) > 0 and not hasattr(result_class, '__id_key__'):
            raise TypeError("Model class does not have a column defined as Primary Key.")

        return result_class

    def __setattr__(self, __name, __value):
        if __name == '__mapping__':
            if isinstance(__value, Dict):
                """When setting the column mapping, if a mapping already exists then the new mapping will be combined
                with the old mapping. This ensures extended models will have a mapping for all inherited fields.
                The mapping will be stored as a MappingProxy to ensure a read-only status. MappingProxyType is imported
                as FrozenDict to convey intended use case."""
                __value = FrozenDict(getattr(self, __name) | __value) if hasattr(self, __name) else FrozenDict(__value)
                setattr(self, '__mapping_inverse__', FrozenDict(dict(map(reversed, __value.items()))))
            else:
                raise TypeError("Column mapping must be a dictionary.")
        elif __name == '__mapping_inverse__':
            if not isinstance(__value, FrozenDict):
                raise TypeError("Inverse column mapping must already be a frozen dictionary.")
        elif __name == '__indexes__':
            __value = FrozenList(getattr(self, __name) + __value) if hasattr(self, __name) else FrozenList(__value)
        super().__setattr__(__name, __value)


class Model(_ICrud, metaclass=ModelMetaClass):
    # _changed_fields: List[str]

    __mapping__: ClassVar[Dict[str, str]]
    __mapping_inverse__: ClassVar[Dict[str, str]]
    __indexes__: ClassVar[List[TableIndex]]
    __table_name__: ClassVar[str]
    __id_key__: ClassVar[str]
    _db: ClassVar[Database]
    _id_col_name: ClassVar[str]
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
        instance._id = instance.__dict__[instance.__id_key__]
        return instance

    def __setattr__(self, __name, __value):
        """Setting a new value on a TableColumn field will set the value directly to the column rather than reassign
        the column reference."""
        if hasattr(self, __name) and isinstance(attr := getattr(self, __name), TableColumn):
            attr.value = __value
            self._changed_fields.add(attr)
        else:
            super().__setattr__(__name, __value)

    @property
    def id(self):
        return self._id

    @property
    def id_col_name(self):
        return self._id_col_name

    @property
    def table_name(self) -> str:
        return self.__table_name__

    @property
    def initialized(self):
        return self.__class__._initialized

    @classmethod
    def _create_table_query(cls) -> CreateTableQueryBuilder:
        # TODO: Implement the rest of this query builder once the class is completed.
        return CreateTableQueryBuilder(cls.__table_name__)

    @classmethod
    def init_table(cls):
        # TODO: NEEDS TO BE TESTED WHEN CreateTableQueryBuilder IS IMPLEMENTED!
        if not hasattr(cls, '_initialized') or cls._initialized is not True:
            print(f'Table "{cls.__table_name__}" is being created!')
            pass
            cls._initialized = True
            pass
            # q = self._create_table_query()
            # result = self._db.exec_query_no_result(q)
            # if isinstance(result, sqlite3.Error):
            #     init_result = DbOperationResult(error=DbOperationError(result))
            # elif isinstance(result, int):
            #     self.__class__._initialized = True
            #     init_result = DbOperationResult()
            # else:
            #     init_result = DbOperationResult(error=DbOperationError("Unknown error encountered."))
            # if not init_result.success:
            #     raise RuntimeError("The database table failed to be created.")

            if len(cls.__indexes__) > 0:
                for index in cls.__indexes__:
                    # TODO: Create and execute CreateIndexQueryBuilders for each index
                    pass
        else:
            print(f'Table "{cls.__table_name__}" HAS ALREADY BEEN created!')

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

    def populate(self, columns_and_values: ValueMapping | sqlite3.Row) -> None:
        for column_name, value in columns_and_values.items():
            if column_name not in self.__mapping_inverse__:
                raise AttributeError(f"No column mapping exists for column {column_name}.")
            getattr(self, self.__mapping_inverse__[column_name]).value = value


@dataclass(frozen=True)
class DbOperationResults(DbOperationResult):
    results: Dict[Type[Model], DbOperationResult] = field(default_factory=dict)


class ModelList(list, _ICrud):
    # _model_type: Type[Model]
    # _index: Dict[Union[str, int, None], Any]

    def __init__(self, model_type: Type[Model]) -> None:
        super().__init__()
        if not isinstance(model_type, type) or not issubclass(model_type, Model):
            raise TypeError("Invalid model type provided.")
        self._model_type = model_type

    def append(self, __object: Model) -> None:
        if not isinstance(__object, self._model_type):
            raise TypeError("Incorrect model type.")
        else:
            super().append(__object)

    def insert(self, __index: int, __object: Model) -> None:
        if not isinstance(__object, self._model_type):
            raise TypeError("Incorrect model type.")
        else:
            super().insert(__index, __object)

    def __add__(self, x: ModelList[Model]):
        if not isinstance(x, ModelList) or x._model_type is not self._model_type:
            raise TypeError("Incorrect model type.")
        else:
            return super().__add__(x)

    def __iadd__(self, x: ModelList[Model]):
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


def _validate_foreign_keys(database: Database):
    """
    Checks that all the foreign keys defined to each database table only reference other tables in the given database.
    Raises sqlite3.IntegrityError upon finding a foreign table that is not assigned to the database.
    :param database: The given database to be validated.
    """
    for model in database.__class__.__tables__:
        for column_name in model.__mapping__:
            column: TableColumn = getattr(model, column_name)
            constraint: ColumnConstraint = column.constraint
            if (isinstance(constraint, ForeignKey)
                    and constraint.foreign_table_class not in database.__class__.__tables__):
                raise sqlite3.IntegrityError(
                    f'Foreign key table `{str(model.__table_name__)}` is not associated with database `{database.__class__.__name__}`.')


def _sort_models(models: List[Type[Model]]):
    """
    Sorts a list of Model classes in order of foreign key dependency.
    :param models: The list of model classes to be sorted.
    :return: A sorted list of model classes with foreign referenced tables coming before the tables that reference them.
    """
    foreign_key_map: Dict[Type[Model], List[Type[Model]]] = {}

    def foreign_models(model: Type[Model]) -> List[Type[Model]]:
        if model not in foreign_key_map:
            foreign_key_map[model] = []
            for column_name in model.__mapping__:
                column: TableColumn = getattr(model, column_name)
                constraint: ColumnConstraint = column.constraint
                if isinstance(constraint, ForeignKey):
                    foreign_key_map[model].append(constraint.foreign_table_class)

        return foreign_key_map[model]

    def compare(a: Type[Model], b: Type[Model]):
        map_a = foreign_models(a)
        map_b = foreign_models(b)
        if a in map_b:
            return -1
        elif b in map_a:
            return 1
        else:
            return 0

    return sorted(models, key=functools.cmp_to_key(compare))
