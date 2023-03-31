from abc import ABC, abstractmethod
from typing import Any, Self, Type

from aluminum.base import Base


class AbstractRegistry(ABC):
    @abstractmethod
    def _autoload(self, base: Type[Base]) -> None:
        ...


class AbstractSelect(ABC):
    @abstractmethod
    def __init__(self, query: tuple[str, ...]) -> None:
        ...

    def where(self, query: tuple[str, ...]) -> Self:
        ...


class AbstractBucket(ABC):
    @abstractmethod
    async def add(self, item: Base) -> None:
        ...

    @abstractmethod
    def to_dict(self) -> dict:
        ...

    @abstractmethod
    async def query(self, select: AbstractSelect) -> list[Base]:
        ...

    @abstractmethod
    async def raw_query(self, select: str) -> Any:
        ...


class AbstractStore(ABC):
    @abstractmethod
    async def healthy(self) -> bool:
        ...

    @abstractmethod
    async def create_bucket(self, model: Type[Base]) -> AbstractBucket:
        ...

    @abstractmethod
    def get_bucket(self, model: Type[Base]) -> AbstractBucket:
        ...

    @abstractmethod
    def get_buckets(self) -> list[AbstractBucket]:
        ...

    @abstractmethod
    async def delete_bucket(self, model: Type[Base]) -> bool:
        ...
