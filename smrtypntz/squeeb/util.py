from enum import Enum


class _IStringable(object):

    def __str__(self) -> str:
        raise NotImplementedError()


class _StringEnum(Enum):

    def __str__(self):
        return str(self.value)