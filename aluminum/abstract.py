from abc import ABC, abstractmethod
from typing import Type

from aluminum.base import Base


class AbstractBucket(ABC):
    @abstractmethod
    async def add(self, item: Base) -> None:
        ...

    @abstractmethod
    def to_dict(self) -> dict:
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


class AbstractRegistry(ABC):
    @abstractmethod
    def _autoload(self, base: Type[Base]) -> None:
        ...
