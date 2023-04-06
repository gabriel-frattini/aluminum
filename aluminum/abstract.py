from abc import ABC, abstractclassmethod, abstractmethod
from typing import Any, Generic, Type, TypeVar

from aluminum.operator import WhereOperator

T = TypeVar("T")
TAbstractSelect = TypeVar("TAbstractSelect", bound="AbstractSelect")


class AbstractResult(ABC):
    @abstractmethod
    async def all(self) -> list:  # noqa: A003, D102
        ...


class AbstractBase(ABC):
    @abstractclassmethod
    def schema(cls) -> dict[Any, Any]:
        ...

    @abstractmethod
    def dict(self) -> dict[Any, Any]:
        ...


class AbstractMapped(Generic[T], ABC):
    _col_name: str


class AbstractWhereClause(ABC, Generic[T]):
    _left_operand: AbstractMapped[T]
    _right_operand: T
    _operator: WhereOperator

    @abstractmethod
    def __init__(self, left_operand: AbstractMapped[T], right_operand, operator):
        ...

    @abstractmethod
    def __str__(self):
        ...

    @abstractmethod
    def get_clause(self) -> tuple[AbstractMapped, WhereOperator, T]:
        ...


class AbstractRegistry(ABC):
    @abstractmethod
    def _autoload(self, AbstractBase: Type[AbstractBase]) -> None:
        ...


class AbstractSelect(ABC):
    @abstractmethod
    def __init__(self, query: tuple[str, ...]) -> None:
        ...

    def where(
        self: TAbstractSelect, *args: AbstractWhereClause[Any]
    ) -> TAbstractSelect:
        ...

    def _get_raw_query(self) -> str:
        ...


class AbstractBucket(ABC):
    @abstractmethod
    async def add(self, item: AbstractBase) -> None:
        ...

    @abstractmethod
    def to_dict(self) -> dict:
        ...

    @abstractmethod
    async def query(self, select: AbstractSelect) -> list[AbstractBase]:
        ...

    @abstractmethod
    async def raw_query(self, select: str) -> Any:
        ...

    @abstractmethod
    async def execute(self, select: AbstractSelect) -> list[AbstractBase]:
        ...


class AbstractStore(ABC):
    @abstractmethod
    async def healthy(self) -> bool:
        ...

    @abstractmethod
    async def create_bucket(self, model: Type[AbstractBase]) -> AbstractBucket:
        ...

    @abstractmethod
    def get_bucket(self, model: Type[AbstractBase]) -> AbstractBucket:
        ...

    @abstractmethod
    def get_buckets(self) -> list[AbstractBucket]:
        ...

    @abstractmethod
    async def delete_bucket(self, model: Type[AbstractBase]) -> bool:
        ...
