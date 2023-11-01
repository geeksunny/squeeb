from enum import StrEnum
from typing import Any, Dict


class Order(StrEnum):
    ASC = "ASC"
    DESC = "DESC"


ValueMapping = Dict[str, Any]
