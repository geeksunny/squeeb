from __future__ import annotations

from dataclasses import dataclass, field, InitVar
from typing import List, Type, TYPE_CHECKING

from squeeb.common import Order
from squeeb.model import TableColumn
from squeeb.util import _IStringable, FrozenList

if TYPE_CHECKING:
    from squeeb.model import Model


@dataclass
class IndexedColumn(_IStringable):
    column: TableColumn
    collation_name: str = None
    sort_order: Order = None

    # TODO: Implement `expr` as alternative to self.column

    def __hash__(self):
        return hash((self.column, self.collation_name, self.sort_order))

    def __str__(self) -> str:
        output = [self.column.column_name]
        if self.collation_name is not None:
            output.append(f'COLLATE {self.collation_name}')
        if self.sort_order is not None:
            output.append(str(self.sort_order))
        return ' '.join(output)


@dataclass(frozen=True)
class TableIndex:
    table_model: Type[Model] = field(init=False)
    _columns: InitVar[List[IndexedColumn | TableColumn]]
    columns: List[IndexedColumn] = field(init=False)
    index_name: str = None
    is_unique: bool = False
    if_not_exists: bool = False

    # https://www.sqlite.org/lang_createindex.html
    # TODO: Implement `WHERE expr`

    def __post_init__(self, _columns: List[IndexedColumn | TableColumn]):
        """Prepping `columns` field."""
        result = []
        for column in _columns:
            if isinstance(column, TableColumn):
                column = IndexedColumn(column)
            elif not isinstance(column, IndexedColumn):
                raise TypeError('Column definition must be of TableColumn or IndexedColumn type.')
            if column not in result:
                result.append(column)
        object.__setattr__(self, 'columns', FrozenList(result))

    def __hash__(self):
        return hash((self.table_model, hash(tuple(self.columns)), self.index_name, self.is_unique, self.if_not_exists))

    def _setup(self, table_model: Type[Model], default_name: str = None):
        object.__setattr__(self, 'table_model', table_model)
        if self.index_name is None:
            if default_name is not None:
                object.__setattr__(self, 'index_name', default_name)
            else:
                names: List[str] = []
                for column in self.columns:
                    names.append(column.column.column_name)
                object.__setattr__(self, 'index_name', f'{self.table_model.table_name}_{'-'.join(names)}')
