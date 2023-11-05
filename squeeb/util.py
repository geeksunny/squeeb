import re
from abc import ABCMeta, abstractmethod


def camel_to_snake_case(value: str, lowercase: bool = False, uppercase: bool = False):
    underscored = re.sub(r'([a-z])([A-Z])', r'\1_\2',
                         re.sub(r'([A-Z])([A-Z][a-z])', r'\1_\2', value))
    if lowercase is True:
        return underscored.lower()
    elif uppercase is True:
        return underscored.upper()
    else:
        return underscored


class _IStringable(object, metaclass=ABCMeta):

    @abstractmethod
    def __str__(self) -> str:
        pass


class Singleton(ABCMeta):
    _instances = {}

    def __call__(cls, *args, **kwds):
        if cls not in cls._instances:
            cls._instances[cls] = super().__call__(*args, **kwds)
        return cls._instances[cls]
