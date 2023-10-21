from typing import List, Iterable, Union, Any

from squeeb.query.queries import Operator
from squeeb.query.values import _QueryValueHandlerMixin, QueryValues
from squeeb.util import _IStringable, _StringEnum


class QueryConditionError(Exception):
    pass


class Junction(_StringEnum):
    AND = " AND "
    OR = " OR "


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
            pass

    def is_ready_for_condition(self) -> bool:
        return isinstance(self._conditions[-1], Junction)

    def where(self, col_name_or_condition: Union[str, _BaseQueryCondition]) -> _MutableConditionMixin:
        if isinstance(col_name_or_condition, _BaseQueryCondition):
            self._conditions.append(col_name_or_condition)
        elif isinstance(col_name_or_condition, str):
            self._conditions.append(MutableQueryCondition(col_name_or_condition))
        else:
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
