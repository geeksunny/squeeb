from typing import Type

from squeeb.db import Database
from squeeb.model.models import Model
from squeeb.util import camel_to_snake_case


def table(cls: Type[Model] = None, db_class: Type[Database] = None, table_name: str = None):
    """
    Decorates a Model subclass to wire up internal dependencies.
    :param cls: The class being generated. This is passed automatically and can be ignored.
    :param db_class: The Database class that this table will be associated with.
    :param table_name: The table name that this model will be represented as in the database.
           The name will default to an all lower-case snake-cased pluralized version of your models name.
           For example: A model class named 'ItemRecord' will become 'item_records'.
    :return: A wrapped subclass of your decorated class definition.
    """
    if cls is not None and not issubclass(cls, Model):
        raise TypeError("Decorated class must be a subclass of Model.")
    if not issubclass(db_class, Database):
        raise TypeError("db_class must be a subclass of Database.")
    if table_name is not None and (not isinstance(table_name, str) or len(table_name) == 0):
        raise TypeError("Invalid table_name provided.")

    def wrap(clss):
        _table_name = table_name if table_name is not None else f'{camel_to_snake_case(clss.__name__, lowercase=True)}s'

        clss.__table_name__ = _table_name
        clss._db = db_class

        print(f'__TABLE CLASS__ . __TABLE NAME__ is {clss.__name__}.{clss.__table_name__}')
        db_class.register_table(clss)
        return clss

    return wrap if cls is None else wrap(cls)
