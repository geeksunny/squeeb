from __future__ import annotations

from abc import ABCMeta, abstractmethod
from typing import TypeVar, Dict, Tuple, Any, Iterable, List, Generator


QueryValues = TypeVar('QueryValues', Tuple[Any, ...], Generator[Tuple[Any, ...], None, None])


class _QueryValueHandlerMixin(object, metaclass=ABCMeta):

    @abstractmethod
    def _get_values(self) -> QueryValues:
        pass

    @property
    def value_args(self) -> QueryValues:
        return self._get_values()


class _IQueryValueStrings(object, metaclass=ABCMeta):

    @property
    @abstractmethod
    def column_str(self) -> str:
        pass

    @property
    @abstractmethod
    def values_str(self) -> str:
        pass


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
