from enum import StrEnum


class Operator(StrEnum):
    EQUALS = '='
    NOT_EQUALS = '!='
    GREATER_THAN = '>'
    GREATER_THAN_EQUALS = '>='
    LESS_THAN = '<'
    LESS_THAN_EQUALS = '<='
    LIKE = 'LIKE'
    GLOB = 'GLOB'
    IN = 'IN'
    NOT_IN = 'NOT IN'
