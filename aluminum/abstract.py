from abc import ABC, abstractclassmethod, abstractmethod
from typing import Any, Generic, Self, Type, TypeVar

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

    @abstractclassmethod
    def __lt__(cls, value: T) -> Any:
        ...

    @abstractclassmethod
    def __le__(cls, value: T) -> Any:
        ...

    @abstractclassmethod
    def __eq__(cls, value: T) -> Any:
        ...

    @abstractclassmethod
    def __ne__(cls, value: T) -> Any:
        ...

    @abstractclassmethod
    def __gt__(cls, value: T) -> Any:
        ...

    @abstractclassmethod
    def __ge__(cls, value: T) -> Any:
        ...


class AbstractWhereClause(ABC, Generic[T]):
    _left_operand: AbstractMapped[T]
    _right_operand: T
    _operator: WhereOperator

    @abstractmethod
    def __str__(self):
        ...


class AbstractRegistry(ABC):
    @abstractmethod
    def _autoload(self, AbstractBase: Type[AbstractBase]) -> None:
        ...


class AbstractSelect(ABC):
    def where(self, *args: AbstractWhereClause[Any]) -> Self:
        ...

    def _where(
        self,
        left_operand: AbstractMapped[Any],
        operator: WhereOperator,
        right_operand: Any,
    ) -> Self:
        ...

    def _create_bucket_str(self, name: str) -> None:
        ...

    def _create_filter_str(self) -> None:
        ...

    def _create_range_str(self) -> None:
        ...

    def _create_raw_query(self) -> str:
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
