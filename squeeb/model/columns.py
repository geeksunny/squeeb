from abc import ABCMeta, abstractmethod
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Tuple, Dict, Type


class DataType(StrEnum):
    NULL = "NULL"
    INTEGER = "INTEGER"
    REAL = "REAL"
    TEXT = "TEXT"
    BLOB = "BLOB"


@dataclass(frozen=True)
class Key:
    class Action(StrEnum):
        NO_ACTION = "NO ACTION"
        RESTRICT = "RESTRICT"
        SET_NULL = "SET NULL"
        SET_DEFAULT = "SET DEFAULT"
        CASCADE = "CASCADE"

    local_column_name: str
    referenced_model: Type[Any]
    referenced_model_column_name: str
    action: Action = None


@dataclass
class TableColumn(metaclass=ABCMeta):
    value: Any
    test: str = None

    @property
    @abstractmethod
    def column_name(self):
        pass

    @property
    @abstractmethod
    def data_type(self):
        pass

    @property
    @abstractmethod
    def key(self):
        pass


__column_classes: Dict[Tuple[DataType, str, Key], Type[TableColumn]] = {}


def column(data_type: DataType, value: Any = None, column_name: str = None, key: Key = None):
    """
    Creates a TableColumnClass with the given parameters. Stores the created class for repeat use.
    :param data_type: Data type of the table column.
    :param value: Initial value of the table column instance.
    :param column_name: Sqlite column name that this field will map to.
           If `None` is provided, the model's member variable name will be used.
    :param key: Optional `Key` object to define a foreign key mapping.
    :return: An instance of the resulting TableColumnClass.
    """
    if (data_type, column_name, key) in __column_classes:
        column_class = __column_classes[(data_type, column_name, key)]
    else:
        class TableColumnClass(TableColumn):

            @property
            def column_name(self):
                return column_name

            @property
            def data_type(self):
                return data_type

            @property
            def key(self):
                return key

        __column_classes[(data_type, column_name, key)] = TableColumnClass
        column_class = TableColumnClass
    return column_class(value)
