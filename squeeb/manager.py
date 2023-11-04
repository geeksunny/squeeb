from __future__ import annotations

import logging
from typing import Dict, TYPE_CHECKING, List

if TYPE_CHECKING:
    from .db import DbHandler
    from .model.models import ModelType

__db_handlers: Dict[str, DbHandler] = {}
__db_models: Dict[str, List[ModelType]] = {}


logger = logging.getLogger()


def _register_db_handler(db_handler: DbHandler, name: str = 'default'):
    if name in __db_handlers:
        logger.warning(f'A database handler has already been registered under the name "{name}".')
    __db_handlers[name] = db_handler
    db_handler._init_tables()


def _get_db_handler(name: str = 'default') -> DbHandler:
    return __db_handlers[name] if name in __db_handlers else None


def _register_table_model(table_model: ModelType, db_handler_name: str = 'default'):
    if db_handler_name not in __db_models:
        __db_models[db_handler_name] = []
    if table_model not in __db_models[db_handler_name]:
        __db_models[db_handler_name].append(table_model)


def _get_table_models(db_handler_name: str = 'default') -> List[ModelType]:
    return __db_models[db_handler_name] if db_handler_name in __db_models else []


def _unregister_table_model(table_model: ModelType, db_handler_name: str = 'default'):
    if db_handler_name in __db_models:
        __db_models[db_handler_name].remove(table_model)
