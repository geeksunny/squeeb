from __future__ import annotations
import string
from collections import namedtuple
from enum import Enum
from typing import Any, Dict, Union, Iterable, List


Query = namedtuple('Query', ['query', 'args', 'error'])


class ConditionError(Exception):
    pass


class _StrEnum(Enum):

    def __str__(self):
        return str(self.value)


class Operator(_StrEnum):
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


class Junction(_StrEnum):
    AND = " AND "
    OR = " OR "


class _QueryValueHandlerMixin(object):

    def _get_values(self) -> Iterable[Any]:
        raise NotImplementedError()

    @property
    def values(self) -> Iterable[Any]:
        return self._get_values()


class _ICondition(_QueryValueHandlerMixin):

    def __str__(self) -> str:
        raise NotImplementedError()


class _IJuncture(object):

    @property
    def and_(self) -> _ConditionSequence:
        raise NotImplementedError()

    @property
    def or_(self) -> _ConditionSequence:
        raise NotImplementedError()


class _BaseCondition(_ICondition):

    _column_name: str = None
    _value: Union[Any, List[Any]] = None
    _operator: Operator = Operator.EQUALS

    def __init__(self, column_name_or_condition: Union[str, _BaseCondition] = None,
                 value: Any = None,
                 operator: Operator = None) -> None:
        if column_name_or_condition is not None and isinstance(column_name_or_condition, _BaseCondition):
            self._column_name = column_name_or_condition._column_name
            self._value = column_name_or_condition._value
            self._operator = column_name_or_condition._operator
        else:
            self._column_name = column_name_or_condition
            self._value = value
            self._operator = operator

    def _get_values(self) -> Iterable[Any]:
        return self._value if isinstance(self._value, list) else [self._value]

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


class Condition(_BaseCondition, _IJuncture):

    @property
    def and_(self) -> _ConditionSequence:
        return _ConditionSequence(self, Junction.AND)

    @property
    def or_(self) -> _ConditionSequence:
        return _ConditionSequence(self, Junction.OR)


class MutableCondition(_BaseCondition, _MutableConditionMixin):

    def _set_condition(self, operator, value) -> Condition:
        if isinstance(value, (list, set, tuple)):
            self._value = []
            self._value.extend(value)
        else:
            self._value = value
        self._operator = operator
        return Condition(self)


class _ConditionSequence(_ICondition):

    _conditions: List[Union[_ICondition, Junction]] = []

    def __init__(self, first_condition_or_sequence: Union[_ICondition, _ConditionSequence],
                 first_junction: Junction = None) -> None:
        if isinstance(first_condition_or_sequence, _ConditionSequence):
            self._conditions = first_condition_or_sequence._conditions
        elif isinstance(first_condition_or_sequence, _ICondition):
            self._conditions.append(first_condition_or_sequence)
            if isinstance(first_junction, Junction):
                self._conditions.append(first_junction)
        else:
            # todo: raise error ?
            pass

    def is_ready_for_condition(self) -> bool:
        return isinstance(self._conditions[-1], Junction)

    def where(self, col_name_or_condition: Union[str, _BaseCondition]) -> _MutableConditionMixin:
        # TODO: expand condition types accepted
        if isinstance(col_name_or_condition, _BaseCondition):
            self._conditions.append(col_name_or_condition)
        elif isinstance(col_name_or_condition, str):
            self._conditions.append(MutableCondition(col_name_or_condition))
        else:
            # todo: raise error
            pass
        return MutableConditionSequence(self)

    def _get_values(self) -> Iterable[Any]:
        values = []
        for condition in self._conditions:
            if isinstance(condition, _ICondition):
                values.extend(condition._get_values())
        return values

    def __str__(self) -> str:
        return "".join(map(str, self._conditions)) if not self.is_ready_for_condition() else ""


class ConditionSequence(_ConditionSequence, _IJuncture):

    @property
    def and_(self) -> _ConditionSequence:
        if not self.is_ready_for_condition():
            self._conditions.append(Junction.AND)
            return self
        else:
            raise ConditionError("Condition sequence out of order.")

    @property
    def or_(self) -> _ConditionSequence:
        if not self.is_ready_for_condition():
            self._conditions.append(Junction.OR)
            return self
        else:
            raise ConditionError("Condition sequence out of order.")


class MutableConditionSequence(_ConditionSequence, _MutableConditionMixin):

    def _set_condition(self, operator, value):
        if isinstance(self._conditions[-1], _MutableConditionMixin):
            self._conditions[-1]._set_condition(operator, value)
        else:
            # todo : raise error
            pass
        return ConditionSequence(self)


class ConditionGroup(list, _ICondition, _IJuncture):

    _conditions: List[_ICondition] = []
    _group_junction: Junction = None

    def __init__(self, group_junction: Junction, conditions: Iterable[_ICondition]) -> None:
        super().__init__()
        self._group_junction = group_junction
        self._conditions.extend(conditions)

    def _get_values(self) -> Iterable[Any]:
        values = []
        for condition in self._conditions:
            values.extend(condition._get_values())
        return values

    def __str__(self) -> str:
        return '(%s)' % (self._group_junction.value.join(map(str, self._conditions)))

    def __join(self, junction: Junction):
        if self._group_junction is junction:
            return self
        else:
            return _ConditionSequence(self, junction)

    @property
    def and_(self) -> _ConditionSequence:
        return self.__join(Junction.AND)

    @property
    def or_(self) -> _ConditionSequence:
        return self.__join(Junction.OR)


class _QueryBuilder(object):

    _table_name: str = None
    _value_map: Dict[str, Any] = None
    _where_conditions: Union[Condition, ConditionSequence, ConditionGroup] = None

    def __init__(self,
                 table_name: str,
                 value_map: Dict[str, Any] = None,
                 where_condition: Union[Condition, ConditionSequence, ConditionGroup] = None) -> None:
        super().__init__()
        self._table_name = table_name
        self._value_map = value_map
        self._where_conditions = where_condition

    def _get_query_str(self) -> str:
        raise NotImplementedError()

    def _get_columns_str(self):
        return ", ".join(self._value_map.keys()) if self._value_map is not None else "*"

    def _get_values_str(self):
        return ", ".join("?"*len(self._value_map)) if self._value_map is not None else ""

    def _get_where_str(self) -> str:
        return 'WHERE %s' % str(self._where_conditions) if self._where_conditions is not None else ""

    def _get_where_args(self) -> List[Any]:
        return self._where_conditions._get_values() if self._where_conditions is not None else []

    def _get_args(self) -> tuple:
        return tuple(self._value_map.values()) if self._value_map is not None else None

    def build(self) -> Query:
        return Query(self._get_query_str(), self._get_args()) if self._table_name is not None\
            else Query(error="No table name provided.")


class _CreateTable(_QueryBuilder):

    def _get_query_str(self) -> str:
        # TODO
        pass


class _DropTable(_QueryBuilder):

    def _get_query_str(self) -> str:
        # TODO
        pass


class InsertQuery(_QueryBuilder):

    def _get_query_str(self) -> str:
        tmpl = string.Template('INSERT INTO $table ($columns) VALUES ($values)')
        return tmpl.substitute({
            "table": self._table_name,
            "columns": self._get_columns_str(),
            "values": self._get_values_str()
        })


class SelectQuery(_QueryBuilder):

    def _get_query_str(self) -> str:
        tmpl = string.Template('SELECT ${columns} FROM ${table} ${where}')
        return tmpl.substitute({
            "columns": self._get_columns_str(),
            "table": self._table_name,
            "where": self._get_where_conditions() if self._where_conditions is not None else ""
        }).strip()


class UpdateQuery(_QueryBuilder):

    def _get_changes_string(self) -> str:
        if self._value_map is None:
            return ''
        changes = []
        for key, val in self._value_map:
            changes.append('%s = %s' % (key, val))
        return ', '.join(changes)

    def _get_query_str(self) -> str:
        tmpl = string.Template('UPDATE ${table} SET ${changes} ${where}')
        return tmpl.substitute({
            "table": self._table_name,
            "changes": self._get_changes_string(),
            "where": self._get_where_conditions() if self._where_conditions is not None else ""
        }).strip()


class DeleteQuery(_QueryBuilder):

    def _get_query_str(self) -> str:
        tmpl = string.Template('DELETE FROM ${table} ${where}')
        return tmpl.substitute({
            "table": self._table_name,
            "where": self._get_where_conditions()
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


def where(*args) -> MutableCondition:
    if len(args) == 1 and type(args[0] is str):
        return MutableCondition(args[0])
    elif len(args) > 1:
        pass
    else:
        # Error state?
        pass


w = where('col_a').equals(1).and_.where('col_b').greater_than(5).or_.where('col_c').not_equals(2)
print('\n%s\n' % w)
print(type(w))
