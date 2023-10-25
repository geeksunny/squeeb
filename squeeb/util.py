from abc import ABCMeta, abstractmethod


class _IStringable(object, metaclass=ABCMeta):

    @abstractmethod
    def __str__(self) -> str:
        pass
