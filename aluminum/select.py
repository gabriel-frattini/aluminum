from typing import Any, TypeVar

from aluminum.abstract import AbstractBase, AbstractSelect
from aluminum.where_clause import WhereClause


def select(*args):
    return Select(*args)


T = TypeVar("T")


TSelect = TypeVar("TSelect", bound="Select")


class Select(AbstractSelect):
    _select: AbstractBase
    _where_clauses: tuple[WhereClause, ...]
    _raw_query: str

    def __init__(self, select) -> None:
        self._select = select

    def where(self: TSelect, *args: WhereClause[Any]) -> TSelect:
        self._where_clauses = args
        return self

    def _create_bucket_str(self) -> None:
        self._raw_query = f'from(bucket: "{self._select.schema()["title"]}")'

    def _create_filter_str(self) -> None:
        for where_clause in self._where_clauses:
            self._raw_query += f' |> filter(fn: (r) => r.{where_clause._left_operand._col_name} {where_clause._operator.value} "{where_clause._right_operand}")'

    def _create_range_str(self) -> None:
        self._raw_query += " |> range(start: -1h)"

    def _get_raw_query(self) -> str:
        self._create_bucket_str()
        self._create_range_str()
        self._create_filter_str()
        return self._raw_query
