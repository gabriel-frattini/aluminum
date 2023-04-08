from typing import Any, TypeVar

from aluminum.abstract import AbstractBase, AbstractSelect
from aluminum.aluminum import _WhereClause, _Select


def select(*args):
    return Select(*args)


T = TypeVar("T")


TSelect = TypeVar("TSelect", bound="Select")


class Select(AbstractSelect):
    _select_bucket: AbstractBase
    _where_clauses: tuple[_WhereClause, ...]
    _raw_query: str
    _select: _Select

    def __init__(self, select: AbstractBase) -> None:
        self._select_bucket = select
        self._select = _Select(select)

    def where(self: TSelect, *args: _WhereClause) -> TSelect:
        for arg in args:
            self._select._where(
                left_operand=arg.get_left_operand(),
                operator=arg.get_operator_str(),
                right_operand=arg.get_right_operand(),
            )
        self._where_clauses = args
        return self

    def _create_bucket_str(self) -> None:
        _bucket_name: str = self._select_bucket.schema()["title"]
        self._select._create_bucket_str(_bucket_name)

    def _create_filter_str(self) -> None:
        self._select._create_filter_str()

    def _create_range_str(self) -> None:
        self._select._create_range_str()

    def _create_raw_query(self):
        self._create_bucket_str()
        self._select._create_raw_query()

    def _get_raw_query(self) -> str:
        return self._select._get_raw_query()
