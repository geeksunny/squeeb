import string
from dataclasses import dataclass
from enum import Enum
from typing import Tuple, Union, Any, Dict, List

from squeeb import QueryCondition
from squeeb.query.conditions import _IQueryCondition, QueryConditionSequence, QueryConditionGroup, MutableQueryCondition
from squeeb.query.values import _QueryValueMap, _QueryValueMapGroup
from squeeb.util import _StringEnum


@dataclass
class Query:
    query: str = None
    args: Tuple[Any, ...] = None
    error: Union[str, Exception] = None


class Operator(_StringEnum):
    EQUALS = '='
    NOT_EQUALS = '!='
    GREATER_THAN = '>'
    GREATER_THAN_EQUALS = '>='
    LESS_THAN = '<'
    LESS_THAN_EQUALS = '<='
    LIKE = 'LIKE'
    GLOB = 'GLOB'
    IN = 'IN'
    NOT_IN = 'NOT IN'


class _QueryArgs(Enum):
    VALUE = 1
    WHERE = 2


class AbstractQueryBuilder:
    pass


class AbstractQueryBuilder(object):

    _table_name: str = None
    _value_map: Union[_QueryValueMap, _QueryValueMapGroup] = None
    _where_conditions: _IQueryCondition = None

    def __init__(self,
                 table_name: str,
                 value_map: Union[Dict[str, Any], List[Dict[str, Any]]] = None,
                 where_condition: Union[QueryCondition, QueryConditionSequence, QueryConditionGroup] = None) -> None:
        super().__init__()
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

    def _get_query_str(self) -> str:
        raise NotImplementedError()

    def _get_args_needed(self) -> Tuple[_QueryArgs]:
        raise NotImplementedError()

    def set_value(self, value_obj: Union[Dict[str, Any], List[Dict[str, Any]]]) -> AbstractQueryBuilder:
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
                args_part = f_map[arg].value_args if arg in f_map and f_map[arg] is not None else None
                if args_part is None or len(args_part) == 0:
                    continue  # Skip
                query_args.extend(args_part)
        return tuple(query_args)

    def build(self) -> Query:
        return Query(self._get_query_str(), self._get_args(self._get_args_needed())) if self._table_name is not None\
            else Query(error="No table name provided.")


class CreateIndexQueryBuilder(AbstractQueryBuilder):

    def _get_args_needed(self) -> Tuple[_QueryArgs]:
        pass

    def _get_query_str(self) -> str:
        tmpl = string.Template('INSERT INTO $table $columns VALUES $values')
        return tmpl.substitute({
            "table": self._table_name,
            "columns": self._get_columns_str(),
            "values": self._get_values_str()
        })


class CreateTableQueryBuilder(AbstractQueryBuilder):

    def _get_args_needed(self) -> Tuple[_QueryArgs]:
        pass

    def _get_query_str(self) -> str:
        pass


class DropTableQueryBuilder(AbstractQueryBuilder):

    def _get_args_needed(self) -> Tuple[_QueryArgs]:
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


def where(*args) -> MutableQueryCondition:
    if len(args) == 1 and type(args[0] is str):
        return MutableQueryCondition(args[0])
    elif len(args) > 1:
        pass
    else:
        pass

