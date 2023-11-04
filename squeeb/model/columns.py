from __future__ import annotations

from abc import ABCMeta, abstractmethod, ABC
from dataclasses import dataclass, InitVar, field
from enum import StrEnum
from typing import Any, Tuple, Dict, Type

import squeeb.model
from squeeb.common import Order
from squeeb.util import _IStringable


class DataType(StrEnum):
    NULL = "NULL"
    INTEGER = "INTEGER"
    REAL = "REAL"
    TEXT = "TEXT"
    BLOB = "BLOB"


class ConflictClause(StrEnum):
    ROLLBACK = "ROLLBACK"
    ABORT = "ABORT"
    FAIL = "FAIL"
    IGNORE = "IGNORE"
    REPLACE = "REPLACE"

    def __str__(self):
        return f'ON CONFLICT {super().__str__()}'


class ColumnConstraint(_IStringable):
    pass


@dataclass(frozen=True)
class PrimaryKey(ColumnConstraint):
    order: Order = None
    conflict_clause: ConflictClause = None
    autoincrement: bool = False
    unique: bool = False

    def __str__(self) -> str:
        output = ['PRIMARY KEY']
        if self.order is not None:
            output.append(self.order)
        if self.conflict_clause is not None:
            output.append(self.conflict_clause)
        if self.autoincrement is True:
            output.append('AUTOINCREMENT')
        if self.unique is True:
            output.append('UNIQUE')
        return ' '.join(output)


class KeyAction(StrEnum):
    NO_ACTION = "NO ACTION"
    RESTRICT = "RESTRICT"
    SET_NULL = "SET NULL"
    SET_DEFAULT = "SET DEFAULT"
    CASCADE = "CASCADE"


@dataclass(frozen=True)
class ForeignKey(ColumnConstraint):
    foreign_table_class: squeeb.model.models.AbstractModel
    foreign_table_column: InitVar[TableColumn]
    foreign_column_name: str = field(init=False)

    on_delete_action: KeyAction = None
    on_update_action: KeyAction = None
    # todo: match [name], [not] deferrable [initially [deferred / immediate]]

    def __post_init__(self, foreign_table_column: TableColumn):
        object.__setattr__(self, 'foreign_column_name', foreign_table_column.column_name)

    def __str__(self) -> str:
        output = [f'REFERENCES "{self.foreign_table_class._table_name}"("{self.foreign_column_name}")']
        if self.on_delete_action is not None:
            output.append(f'ON DELETE {self.on_delete_action}')
        if self.on_update_action is not None:
            output.append(f'ON UPDATE {self.on_update_action}')
        return ' '.join(output)


@dataclass(frozen=True)
class ConflictClauseConstraint(ColumnConstraint, ABC):
    conflict_clause: ConflictClause = None

    @property
    @abstractmethod
    def keyword(self):
        pass

    def __str__(self) -> str:
        output = [self.keyword]
        if self.conflict_clause is not None:
            output.append(self.conflict_clause)
        return ' '.join(output)


class NotNull(ConflictClauseConstraint):

    @property
    def keyword(self):
        return 'NOT NULL'


class Unique(ConflictClauseConstraint):

    @property
    def keyword(self):
        return 'UNIQUE'


class CollateSequence(StrEnum):
    BINARY = "BINARY"
    NOCASE = "NOCASE"
    RTRIM = "RTRIM"


@dataclass
class Collate(ColumnConstraint):
    sequence: CollateSequence

    def __str__(self):
        return f'COLLATE {self.sequence}' if isinstance(self.sequence, CollateSequence) else ''


class Defaults(StrEnum):
    CURRENT_TIME = "CURRENT_TIME"  # "HH:MM:SS"
    CURRENT_DATE = "CURRENT_DATE"  # "YYYY-MM-DD"
    CURRENT_TIMESTAMP = "CURRENT_TIMESTAMP"  # "YYYY-MM-DD HH:MM:SS"
    NULL = "NULL"


@dataclass(frozen=True)
class DefaultValue(ColumnConstraint):
    value: Any = None

    def __str__(self) -> str:
        return f'DEFAULT {self.value}' if self.value is not None else ''


class DefaultExpression(DefaultValue):

    def __getattribute__(self, __name):
        if __name == 'value':
            return f'({super().__getattribute__(__name)})'
        return super().__getattribute__(__name)


@dataclass
class TableColumn(metaclass=ABCMeta):
    value: Any

    def __hash__(self):
        return hash((self.column_name, self.data_type, self.constraints, self.value))

    @property
    @abstractmethod
    def column_name(self) -> str:
        pass

    @property
    @abstractmethod
    def data_type(self) -> DataType:
        pass

    @property
    @abstractmethod
    def constraints(self) -> Tuple[ColumnConstraint, ...]:
        pass


__column_classes: Dict[Tuple[DataType, str, Tuple[ColumnConstraint, ...]], Type[TableColumn]] = {}


def column(data_type: DataType, value: Any = None, column_name: str = None,
           constraints: Tuple[ColumnConstraint, ...] | ColumnConstraint = None):
    """
    Creates a TableColumnClass with the given parameters. Stores the created class for repeat use.
    :param data_type: Data type of the table column.
    :param value: Initial value of the table column instance.
    :param column_name: Sqlite column name that this field will map to.
           If `None` is provided, the model's member variable name will be used.
    :param constraints: Optional tuple of ColumnConstraint objects to attach to this column.
           May also provide a single ColumnConstraint object.
    :return: An instance of the resulting TableColumnClass.
    """
    if not isinstance(constraints, tuple):
        constraints = (constraints, )
    if (data_type, column_name, constraints) in __column_classes:
        column_class = __column_classes[(data_type, column_name, constraints)]
    else:
        class TableColumnClass(TableColumn):

            @property
            def column_name(self) -> str:
                return column_name

            @property
            def data_type(self) -> DataType:
                return data_type

            @property
            def constraints(self) -> Tuple[ColumnConstraint, ...]:
                return constraints

        __column_classes[(data_type, column_name, constraints)] = TableColumnClass
        column_class = TableColumnClass
    return column_class(value)
