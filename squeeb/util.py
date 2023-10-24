from abc import ABCMeta, abstractmethod
from enum import Enum


class _IStringable(object, metaclass=ABCMeta):

    @abstractmethod
    def __str__(self) -> str:
        pass


class _StringEnum(Enum):

    def __str__(self):
        return str(self.value)
