import re
from abc import ABCMeta, abstractmethod
from typing import get_type_hints, List, ClassVar, get_origin


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


class ProtectedClassVarsMeta(type):
    """
    A metaclass that manages class variable assignments. When using this metaclass, any class variables type-hinted
    as ClassVar will only be assignable once. Any subsequent attempts to assign these variables will raise a TypeError.
    """
    __class_vars: List[str] = []

    def __new__(metacls, cls, bases, classdict, **kwargs):
        result_class = super().__new__(metacls, cls, bases, classdict, **kwargs)
        hints = get_type_hints(result_class)
        for k, v in hints.items():
            if get_origin(v) is ClassVar:
                metacls.__class_vars.append(k)
        return result_class

    def __setattr__(self, __name, __value):
        if __name in self.__class_vars and hasattr(self, __name):
            raise TypeError(f'{self.__name__}.{__name} cannot be overwritten once it has been set.')
        super().__setattr__(__name, __value)


class ABCProtectedClassVarsMeta(ABCMeta, ProtectedClassVarsMeta):
    """Composite metaclass that combines Abstract Base Classes with ProtectedClassVarsMeta functionality."""
    pass
