from typing import TypeVar

from aluminum.abstract import AbstractMappedColumn
from aluminum.where_clause import WhereClause, WhereOperator

T = TypeVar("T")


class MappedColumn(AbstractMappedColumn[T]):
    _col_name: str

    def __init__(self, col_name: str):
        self._col_name = col_name

    def __lt__(self, value):
        return WhereClause(self, value, WhereOperator.LT)

    def __le__(self, value):
        return WhereClause(self, value, WhereOperator.LE)

    def __eq__(self, value):
        return WhereClause(self, value, WhereOperator.EQ)

    def __ne__(self, value):
        return WhereClause(self, value, WhereOperator.NE)

    def __ge__(self, value):
        return WhereClause(self, value, WhereOperator.GE)

    def __gt__(self, value):
        return WhereClause(self, value, WhereOperator.GT)


def mapped_column(col_name: str) -> MappedColumn[T]:
    return MappedColumn(col_name)
