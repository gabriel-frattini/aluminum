from typing import Generic, TypeVar

from aluminum.abstract import AbstractMapped
from aluminum.aluminum import _Mapped

T = TypeVar("T")


class Mapped(AbstractMapped, Generic[T]):
    _col_name: str
    _mapped: _Mapped

    def __init__(self, col_name: str):
        self._mapped = _Mapped(col_name)

    def __lt__(self, value):
        return self._mapped.__lt__(value)

    def __le__(self, value):
        return self._mapped.__le__(value)

    def __eq__(self, value):
        return self._mapped.__eq__(value)

    def __ne__(self, value):
        return self._mapped.__ne__(value)

    def __gt__(self, value):
        return self._mapped.__gt__(value)

    def __ge__(self, value):
        return self._mapped.__ge__(value)

    def _get_col_name(self) -> str:
        return self._mapped._get_col_name()


def mapped_column(col_name: str) -> Mapped[T]:
    return Mapped(col_name)
