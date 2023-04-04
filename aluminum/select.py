from typing import TypeVar
from aluminum.abstract import AbstractSelect

from aluminum.where_clause import WhereClause


def select(*args):
    return Select(*args)


T = TypeVar("T")


TSelect = TypeVar("TSelect", bound="Select")


class Select(AbstractSelect):
    selects: tuple
    where_clauses: tuple[WhereClause, ...]

    def __init__(self, selects) -> None:
        self.selects = selects

    def where(self: TSelect, *args: WhereClause) -> TSelect:
        self.where_clauses = args
        return self

    def _create_bucket_str(self, bucket_name: str) -> str:
        return f'from(bucket: "{bucket_name}")'
