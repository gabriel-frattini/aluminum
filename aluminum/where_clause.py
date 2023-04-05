from enum import Enum
from typing import Generic, TypeVar

from aluminum.abstract import AbstractMappedColumn


class WhereOperator(Enum):
    LT = "lt"
    LE = "le"
    GT = "gt"
    GE = "ge"
    EQ = "eq"
    NE = "ne"


WHERE_OPERATORS = {
    WhereOperator.LT: "<",
    WhereOperator.LE: "<=",
    WhereOperator.GT: ">",
    WhereOperator.GE: ">=",
    WhereOperator.EQ: "=",
    WhereOperator.NE: "!=",
}

T = TypeVar("T")


class WhereClause(Generic[T]):
    left_operand: AbstractMappedColumn[T]
    right_operand: T
    operator: WhereOperator

    def __init__(self, left_operand: AbstractMappedColumn[T], right_operand, operator):
        self.left_operand = left_operand
        self.right_operand = right_operand
        self.operator = operator

    def __str__(self):
        operator = WHERE_OPERATORS[self.operator]
        return f"{self.left_operand._col_name} {operator} {self.right_operand}"
