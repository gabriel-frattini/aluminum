from abc import ABC, abstractmethod
from typing import Any, Generic, Type, TypeVar


T = TypeVar("T")
TAbstractSelect = TypeVar("TAbstractSelect", bound="AbstractSelect")


class AbstractResult(ABC):
    @abstractmethod
    def all(self) -> list:  # noqa: A003, D102
        ...


class AbstractBase(ABC):
    ...


class AbstractMappedColumn(Generic[T], ABC):
    _col_name: str


class AbstractRegistry(ABC):
    @abstractmethod
    def _autoload(self, AbstractBase: Type[AbstractBase]) -> None:
        ...


class AbstractSelect(ABC):
    @abstractmethod
    def __init__(self, query: tuple[str, ...]) -> None:
        ...

    def where(self: TAbstractSelect, query: tuple[str, ...]) -> TAbstractSelect:
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
    async def execute(self, select: AbstractSelect) -> AbstractResult:
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
