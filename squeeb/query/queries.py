from __future__ import annotations

import string
from abc import ABCMeta, abstractmethod
from dataclasses import dataclass
from enum import Enum, StrEnum
from typing import Tuple, Any, List, TypeVar, Self, Type, TYPE_CHECKING

from squeeb.common import ValueMapping
from squeeb.query.conditions import _IQueryCondition, QueryConditionSequence, QueryConditionGroup, \
    MutableQueryCondition, QueryCondition
from squeeb.query.values import _QueryValueMap, _QueryValueMapGroup

if TYPE_CHECKING:
    from squeeb.model.index import TableIndex
    from squeeb.model.models import Model, TableColumn


@dataclass
class Query:
    query: str = None
    args: Tuple[Any, ...] = None
    error: str | Exception = None



class _QueryArgs(Enum):
    VALUE = 1
    WHERE = 2


class AbstractQueryBuilder(object, metaclass=ABCMeta):
    _table_name: str = None
    _value_map: _QueryValueMap | _QueryValueMapGroup = None
    _where_conditions: _IQueryCondition = None

    def __init__(self,
                 table_name: str,
                 value_map: ValueMapping | List[ValueMapping] = None,
                 where_condition: QueryCondition | QueryConditionSequence | QueryConditionGroup = None) -> None:
        self._table_name = table_name
        if value_map is not None:
            self.set_value(value_map)
        self._where_conditions = where_condition

    def __setattr__(self, name: str, value: Any) -> None:
        if name == "where":
            if not isinstance(value, _IQueryCondition):
                raise TypeError("Invalid value for `where conditions`.")
            self._where_conditions = value
        else:
            super().__setattr__(name, value)

    @abstractmethod
    def _get_args_needed(self) -> Tuple[_QueryArgs] | tuple:
        pass

    @abstractmethod
    def _get_query_str(self) -> str:
        pass

    def set_value(self, value_obj: ValueMapping | List[ValueMapping]) -> Self:
        if isinstance(value_obj, list):
            self._value_map = _QueryValueMapGroup.create(value_obj)
        else:
            self._value_map = _QueryValueMap(value_obj)
        return self

    def _get_columns_str(self) -> str:
        return self._value_map.column_str if self._value_map is not None else "*"

    def _get_values_str(self) -> str:
        return self._value_map.values_str if self._value_map is not None else ""

    def _get_value_set_str(self) -> str:
        return self._value_map.value_set_str if isinstance(self._value_map, _QueryValueMap) else ""

    def _get_where_str(self, prefix: str = 'WHERE') -> str:
        return ('%s %s' % (prefix, str(self._where_conditions))).strip() if self._where_conditions is not None else ""

    def _get_where_args(self) -> List[Any]:
        return self._where_conditions.value_args if self._where_conditions is not None else []

    def _get_args(self, q_args: List[_QueryArgs]) -> Tuple[Any]:
        f_map = {
            _QueryArgs.VALUE: self._value_map,
            _QueryArgs.WHERE: self._where_conditions
        }
        query_args = []
        for arg in q_args:
            if isinstance(arg, _QueryArgs):
                args_part = list(f_map[arg].value_args) if arg in f_map and f_map[arg] is not None else None
                if args_part is None or len(args_part) == 0:
                    continue  # Skip
                query_args.extend(args_part)
        return tuple(query_args)

    def build(self) -> Query:
        return Query(self._get_query_str(), self._get_args(self._get_args_needed())) if self._table_name is not None \
            else Query(error="No table name provided.")


QueryBuilder = TypeVar("QueryBuilder", bound=AbstractQueryBuilder)


class TransactionBehavior(StrEnum):
    DEFERRED = "DEFERRED"
    IMMEDIATE = "IMMEDIATE"
    EXCLUSIVE = "EXCLUSIVE"


class BeginTransactionQueryBuilder(AbstractQueryBuilder):

    def __init__(self, behavior: TransactionBehavior = None) -> None:
        super().__init__('')
        self._behavior = behavior

    def _get_args_needed(self) -> Tuple[_QueryArgs] | tuple:
        return ()

    def _get_query_str(self) -> str:
        return f'BEGIN {self._behavior.value if self._behavior is not None else ''} TRANSACTION'


class CommitQueryBuilder(AbstractQueryBuilder):

    def __init__(self) -> None:
        super().__init__('')

    def _get_args_needed(self) -> Tuple[_QueryArgs] | tuple:
        return ()

    def _get_query_str(self) -> str:
        return 'COMMIT TRANSACTION'


class RollbackQueryBuilder(AbstractQueryBuilder):

    def __init__(self, savepoint_name: str = None) -> None:
        super().__init__('')
        self._savepoint_name = savepoint_name

    def _get_args_needed(self) -> Tuple[_QueryArgs] | tuple:
        return ()

    def _get_query_str(self) -> str:
        query = 'ROLLBACK'
        if self._savepoint_name is not None:
            query += f' TO {self._savepoint_name}'
        return query


class SavepointQueryBuilder(AbstractQueryBuilder):

    def __init__(self, savepoint_name: str) -> None:
        super().__init__('')
        self._savepoint_name = savepoint_name

    def _get_args_needed(self) -> Tuple[_QueryArgs] | tuple:
        return ()

    def _get_query_str(self) -> str:
        return f'SAVEPOINT {self._savepoint_name}'


class ReleaseQueryBuilder(AbstractQueryBuilder):

    def __init__(self, savepoint_name: str) -> None:
        super().__init__('')
        self._savepoint_name = savepoint_name

    def _get_args_needed(self) -> Tuple[_QueryArgs] | tuple:
        return ()

    def _get_query_str(self) -> str:
        return f'RELEASE {self._savepoint_name}'


class CreateIndexQueryBuilder(AbstractQueryBuilder):

    def __init__(self, table_index: TableIndex) -> None:
        super().__init__(table_index.table_model.__table_name__)
        self._index: TableIndex = table_index
        self._unique: bool = table_index.is_unique
        self._if_not_exists: bool = table_index.if_not_exists

    def unique(self) -> Self:
        self._unique = True
        return self

    def if_not_exists(self) -> Self:
        self._if_not_exists = True
        return self

    def _get_columns_str(self) -> str:
        columns = []
        for column in self._index.columns:
            columns.append(f'"{column.column.column_name}"')
        return ', '.join(columns)

    def _get_args_needed(self) -> Tuple[_QueryArgs] | tuple:
        return ()

    def _get_query_str(self) -> str:
        tmpl = string.Template('CREATE $unique INDEX $if_not_exists "$name" ON "$table" ($columns)')
        return tmpl.substitute({
            "unique": 'UNIQUE' if self._unique is True else '',
            "if_not_exists": 'IF NOT EXISTS' if self._if_not_exists is True else '',
            "name": self._index.index_name,
            "table": self._table_name,
            "columns": self._get_columns_str()
        })


class CreateTableQueryBuilder(AbstractQueryBuilder):
    _table_model: Type[Model]
    _is_temporary: bool = False
    _if_not_exists: bool = False
    _strict: bool = False
    _without_rowid: bool = False

    def __init__(self, table_model: Type[Model], is_temporary: bool = False, if_not_exists: bool = False,
                 strict: bool = False, without_rowid: bool = False) -> None:
        super().__init__('')
        self._table_model = table_model
        # TODO: Move `is_temporary`, `strict`, `without_rowid` into Model class.
        self._is_temporary = is_temporary
        self._if_not_exists = if_not_exists
        self._strict = strict
        self._without_rowid = without_rowid

    def if_not_exists(self) -> Self:
        self._if_not_exists = True
        return self

    def temp_table(self) -> Self:
        self._is_temporary = True
        return self

    def strict(self) -> Self:
        self._strict = True
        return self

    def without_rowid(self) -> Self:
        self._without_rowid = True
        return self

    def _get_columns_str(self) -> str:
        columns = []
        for column_name in self._table_model.__mapping__:
            column: TableColumn = getattr(self._table_model, column_name)
            columns.append(str(column))
        return ', '.join(columns)

    def _get_args_needed(self) -> Tuple[_QueryArgs] | tuple:
        return ()

    def _get_query_str(self) -> str:
        tmpl = string.Template('CREATE $temp TABLE $if_not_exists $table ($columns) $options')
        table_options = []
        if self._strict is True:
            table_options.append('STRICT')
        if self._without_rowid is True:
            table_options.append('WITHOUT ROWID')
        return tmpl.substitute({
            "temp": 'TEMPORARY' if self._is_temporary is True else '',
            "if_not_exists": 'IF NOT EXISTS' if self._if_not_exists is True else '',
            "table": self._table_name,
            "columns": self._get_columns_str(),
            "options": ', '.join(table_options)
        })


class DropTableQueryBuilder(AbstractQueryBuilder):

    def _get_args_needed(self) -> Tuple[_QueryArgs] | tuple:
        pass

    def _get_query_str(self) -> str:
        pass


class InsertQueryBuilder(AbstractQueryBuilder):

    def _get_args_needed(self) -> List[_QueryArgs]:
        return [_QueryArgs.VALUE]

    def _get_query_str(self) -> str:
        tmpl = string.Template('INSERT INTO $table $columns VALUES $values')
        return tmpl.substitute({
            "table": self._table_name,
            "columns": self._get_columns_str(),
            "values": self._get_values_str()
        })


class SelectQueryBuilder(AbstractQueryBuilder):

    def _get_args_needed(self) -> List[_QueryArgs]:
        return [_QueryArgs.WHERE]

    def _get_query_str(self) -> str:
        tmpl = string.Template('SELECT $columns FROM $table $where')
        return tmpl.substitute({
            "columns": self._get_columns_str(),
            "table": self._table_name,
            "where": self._get_where_str()
        }).strip()


class UpdateQueryBuilder(AbstractQueryBuilder):

    def _get_args_needed(self) -> List[_QueryArgs]:
        return [_QueryArgs.VALUE, _QueryArgs.WHERE]

    def _get_query_str(self) -> str:
        tmpl = string.Template('UPDATE $table SET $changes $where')
        return tmpl.substitute({
            "table": self._table_name,
            "changes": self._get_value_set_str(),
            "where": self._get_where_str()
        }).strip()


class DeleteQueryBuilder(AbstractQueryBuilder):

    def _get_args_needed(self) -> List[_QueryArgs]:
        return [_QueryArgs.WHERE]

    def _get_query_str(self) -> str:
        tmpl = string.Template('DELETE FROM $table $where')
        return tmpl.substitute({
            "table": self._table_name,
            "where": self._get_where_str()
        }) if self._where_conditions is not None else ''


class PragmaQueryBuilder(AbstractQueryBuilder):

    def __init__(self, command: str, target: str = None, value: Any = None) -> None:
        super().__init__('')
        self._command = command
        self._target = target
        self._value = value

    def _get_args_needed(self) -> Tuple[_QueryArgs] | tuple:
        return []

    def _get_query_str(self) -> str:
        if self._target is not None:
            argument = f'({self._target})'
        elif self._value is not None:
            # TODO: Implement a way to manage quoted vs non-quoted string values.
            argument = f'={self._value}'
        else:
            argument = ''
        command = f'PRAGMA {self._command}{argument}'
        return command


def where(*args) -> MutableQueryCondition:
    if len(args) == 1 and type(args[0]) is str:
        return MutableQueryCondition(args[0])
    elif len(args) > 1:
        pass
    else:
        pass

