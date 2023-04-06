from typing import Generic, TypeVar

from aluminum.abstract import AbstractMapped, AbstractWhereClause
from aluminum.operator import WhereOperator

T = TypeVar("T")


class WhereClause(AbstractWhereClause[T]):
    _left_operand: AbstractMapped[T]
    _right_operand: T
    _operator: WhereOperator

    def __init__(self, left_operand: AbstractMapped[T], right_operand, operator):
        self._left_operand = left_operand
        self._right_operand = right_operand
        self._operator = operator

    def __str__(self):
        return f"{self._left_operand._col_name} {self._operator.value()} {self._right_operand}"

    def get_clause(self) -> tuple[AbstractMapped, WhereOperator, T]:
        return self._left_operand, self._operator, self._right_operand
