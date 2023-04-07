from typing import Any, TypeVar

from aluminum.abstract import AbstractBase, AbstractSelect
from aluminum.aluminum import _WhereClause, _Select, get_schema


def select(*args):
    return Select(*args)


T = TypeVar("T")


TSelect = TypeVar("TSelect", bound="Select")


class Select(AbstractSelect):
    _select: AbstractBase
    _where_clauses: tuple[_WhereClause, ...]
    _raw_query: str
    _select_engine: _Select

    def __init__(self, select: AbstractBase) -> None:
        self._select = select
        self._select_engine = _Select(select)

    def where(self: TSelect, *args: _WhereClause) -> TSelect:
        for arg in args:
            self._select_engine._where(
                left_operand=arg.get_left_operand(),
                operator=arg.get_operator_str(),
                right_operand=arg.get_right_operand(),
            )
        self._where_clauses = args
        return self

    def _create_bucket_str(self) -> None:
        _bucket_name: str = self._select.schema()["title"]
        self._select_engine._create_bucket_str(_bucket_name)

    def _create_filter_str(self) -> None:
        self._select_engine._create_filter_str()

    def _create_range_str(self) -> None:
        self._select_engine._create_range_str()

    def _create_raw_query(self):
        self._create_bucket_str()
        self._select_engine._create_raw_query()

    def _get_raw_query(self) -> str:
        return self._select_engine._get_raw_query()
