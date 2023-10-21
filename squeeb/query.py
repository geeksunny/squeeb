from __future__ import annotations

import string
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Iterable, List, Tuple, Union, Generator, TypeVar

from .util import _IStringable, _StringEnum


@dataclass
class Query:
    query: str = None
    args: Tuple[Any, ...] = None
    error: Union[str, Exception] = None


class QueryConditionError(Exception):
    pass


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


class Junction(_StringEnum):
    AND = " AND "
    OR = " OR "


QueryValues = TypeVar('QueryValues', Tuple[Any, ...], Generator[Tuple[Any, ...], None, None])


class _QueryValueHandlerMixin(object):

    def _get_values(self) -> QueryValues:
        raise NotImplementedError()

    @property
    def value_args(self) -> QueryValues:
        return self._get_values()


class _IQueryValueStrings(object):

    @property
    def column_str(self) -> str:
        raise NotImplementedError()

    @property
    def values_str(self) -> str:
        raise NotImplementedError()


class _IQueryCondition(_IStringable, _QueryValueHandlerMixin):
    pass


class _IQueryJuncture(object):

    @property
    def and_(self) -> _BaseQueryConditionSequence:
        raise NotImplementedError()

    @property
    def or_(self) -> _BaseQueryConditionSequence:
        raise NotImplementedError()


class _BaseQueryCondition(_IQueryCondition):

    _column_name: str = None
    _value: Union[Any, List[Any]] = None
    _operator: Operator = Operator.EQUALS

    def __init__(self, column_name_or_condition: Union[str, _BaseQueryCondition] = None,
                 value: Any = None,
                 operator: Operator = None) -> None:
        if column_name_or_condition is not None and isinstance(column_name_or_condition, _BaseQueryCondition):
            self._column_name = column_name_or_condition._column_name
            self._value = column_name_or_condition._value
            self._operator = column_name_or_condition._operator
        else:
            self._column_name = column_name_or_condition
            self._value = value
            self._operator = operator

    def _get_values(self) -> QueryValues:
        return tuple(self._value) if isinstance(self._value, list) else (self._value, )

    def __str__(self) -> str:
        if self._column_name is None or self._value is None:
            return ''
        value = "(%s)" % ", ".join("?" * len(self._value)) \
            if self._operator in (Operator.IN, Operator.NOT_IN) and isinstance(self._value, list) \
            else "?"
        return '%s %s %s' % (self._column_name, self._operator.value, value)


class _MutableConditionMixin(object):

    def _set_condition(self, operator, value):
        raise NotImplementedError()

    def equals(self, value):
        return self._set_condition(Operator.EQUALS, value)

    def not_equals(self, value):
        return self._set_condition(Operator.NOT_EQUALS, value)

    def greater_than(self, value):
        return self._set_condition(Operator.GREATER_THAN, value)

    def greater_than_equals(self, value):
        return self._set_condition(Operator.GREATER_THAN_EQUALS, value)

    def less_than(self, value):
        return self._set_condition(Operator.LESS_THAN, value)

    def less_than_equals(self, value):
        return self._set_condition(Operator.LESS_THAN_EQUALS, value)

    def like(self, value_template):
        return self._set_condition(Operator.LIKE, value_template)

    def is_in(self, *values):
        v = values[0] if isinstance(values[0], (list, set, tuple)) else values
        return self._set_condition(Operator.IN, v)

    def is_not_in(self, *values):
        v = values[0] if isinstance(values[0], (list, set, tuple)) else values
        return self._set_condition(Operator.NOT_IN, v)


class QueryCondition(_BaseQueryCondition, _IQueryJuncture):

    @property
    def and_(self) -> _BaseQueryConditionSequence:
        return _BaseQueryConditionSequence(self, Junction.AND)

    @property
    def or_(self) -> _BaseQueryConditionSequence:
        return _BaseQueryConditionSequence(self, Junction.OR)


class MutableQueryCondition(_BaseQueryCondition, _MutableConditionMixin):

    def _set_condition(self, operator, value) -> QueryCondition:
        if isinstance(value, (list, set, tuple)):
            self._value = []
            self._value.extend(value)
        else:
            self._value = value
        self._operator = operator
        return QueryCondition(self)


class _BaseQueryConditionSequence(_IQueryCondition):

    _conditions: List[Union[_IQueryCondition, Junction]] = []

    def __init__(self, first_condition_or_sequence: Union[_IQueryCondition, _BaseQueryConditionSequence],
                 first_junction: Junction = None) -> None:
        if isinstance(first_condition_or_sequence, _BaseQueryConditionSequence):
            self._conditions = first_condition_or_sequence._conditions
        elif isinstance(first_condition_or_sequence, _IQueryCondition):
            self._conditions.append(first_condition_or_sequence)
            if isinstance(first_junction, Junction):
                self._conditions.append(first_junction)
        else:
            # todo: raise error ?
            pass

    def is_ready_for_condition(self) -> bool:
        return isinstance(self._conditions[-1], Junction)

    def where(self, col_name_or_condition: Union[str, _BaseQueryCondition]) -> _MutableConditionMixin:
        # TODO: expand condition types accepted
        if isinstance(col_name_or_condition, _BaseQueryCondition):
            self._conditions.append(col_name_or_condition)
        elif isinstance(col_name_or_condition, str):
            self._conditions.append(MutableQueryCondition(col_name_or_condition))
        else:
            # todo: raise error
            pass
        return MutableQueryConditionSequence(self)

    def _get_values(self) -> QueryValues:
        values = []
        for condition in self._conditions:
            if isinstance(condition, _IQueryCondition):
                values.extend(condition.value_args)
        return tuple(values)

    def __str__(self) -> str:
        return "".join(map(str, self._conditions)) if not self.is_ready_for_condition() else ""


class QueryConditionSequence(_BaseQueryConditionSequence, _IQueryJuncture):

    @property
    def and_(self) -> _BaseQueryConditionSequence:
        if not self.is_ready_for_condition():
            self._conditions.append(Junction.AND)
            return self
        else:
            raise QueryConditionError("Condition sequence out of order.")

    @property
    def or_(self) -> _BaseQueryConditionSequence:
        if not self.is_ready_for_condition():
            self._conditions.append(Junction.OR)
            return self
        else:
            raise QueryConditionError("Condition sequence out of order.")


class MutableQueryConditionSequence(_BaseQueryConditionSequence, _MutableConditionMixin):

    def _set_condition(self, operator, value):
        if isinstance(self._conditions[-1], _MutableConditionMixin):
            self._conditions[-1]._set_condition(operator, value)
        else:
            # todo : raise error
            pass
        return QueryConditionSequence(self)


class QueryConditionGroup(list, _IQueryCondition, _IQueryJuncture):

    _conditions: List[_IQueryCondition] = []
    _group_junction: Junction = None

    def __init__(self, group_junction: Junction, conditions: Iterable[_IQueryCondition]) -> None:
        super().__init__()
        self._group_junction = group_junction
        self._conditions.extend(conditions)

    def _get_values(self) -> QueryValues:
        values = []
        for condition in self._conditions:
            values.extend(condition.value_args)
        return values

    def __str__(self) -> str:
        return '(%s)' % (self._group_junction.value.join(map(str, self._conditions)))

    def __join(self, junction: Junction):
        if self._group_junction is junction:
            return self
        else:
            return _BaseQueryConditionSequence(self, junction)

    @property
    def and_(self) -> _BaseQueryConditionSequence:
        return self.__join(Junction.AND)

    @property
    def or_(self) -> _BaseQueryConditionSequence:
        return self.__join(Junction.OR)


class _QueryValueMap(dict, _QueryValueHandlerMixin, _IQueryValueStrings):

    def __init__(self, values: Dict[str, Any]) -> None:
        super().__init__()
        self.update(values)

    def _get_values(self) -> Tuple[Any, ...]:
        return tuple(self.values())

    @property
    def column_str(self) -> str:
        return "(%s)" % ", ".join(self.keys())

    @property
    def values_str(self) -> str:
        return "(%s)" % ", ".join("?" * len(self))

    @property
    def value_set_str(self) -> str:
        return ", ".join(map(lambda key: "%s = ?" % key, self.keys()))


class _QueryValueMapGroup(_QueryValueHandlerMixin, _IQueryValueStrings):

    _value_maps: List[_QueryValueMap] = []

    @staticmethod
    def create(value_maps: Iterable[Dict[str, Any]]) -> _QueryValueMapGroup:
        query_value_maps = []
        for value_map in value_maps:
            query_value_maps.append(_QueryValueMap(value_map))
        return _QueryValueMapGroup(query_value_maps)

    def __init__(self, query_value_maps: Iterable[_QueryValueMap] = None) -> None:
        for value_map in query_value_maps:
            # Type check happens in .add()
            self.add(value_map)

    def add(self, value_map: _QueryValueMap) -> None:
        if not isinstance(value_map, _QueryValueMap):
            raise TypeError("Object must be a _QueryValueMap")
        self._value_maps.append(value_map)

    def _get_values(self) -> Generator[Tuple[Any, ...]]:
        for value_map in self._value_maps:
            yield value_map.value_args

    @property
    def column_str(self) -> str:
        # Uses column str from first value_map object.
        return self._value_maps[0].column_str if len(self._value_maps) > 0 else ""

    @property
    def values_str(self) -> str:
        return ", ".join(map(lambda vm: vm.values_str, self._value_maps))


class _QueryArgs(Enum):
    VALUE = 1
    WHERE = 2


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


class CreateTableQueryBuilder(AbstractQueryBuilder):

    def _get_args_needed(self) -> Tuple[_QueryArgs]:
        pass

    def _get_query_str(self) -> str:
        # TODO
        pass


class DropTableQueryBuilder(AbstractQueryBuilder):

    def _get_args_needed(self) -> Tuple[_QueryArgs]:
        pass

    def _get_query_str(self) -> str:
        # TODO
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


def and_group(*args):
    if len(args) > 1:
        pass
    else:
        pass


def or_group(*args):
    if len(args) > 1:
        pass
    else:
        pass


def where(*args) -> MutableQueryCondition:
    if len(args) == 1 and type(args[0] is str):
        return MutableQueryCondition(args[0])
    elif len(args) > 1:
        pass
    else:
        # Error state?
        pass
