from typing import Optional, Type
from aluminum.base import Base
from aluminum.engine import Engine
from aluminum.aluminum import _Store, _Registry
from aluminum.abstract import AbstractBucket, AbstractRegistry, AbstractStore


class Store:

    _store: AbstractStore
    _registry: AbstractRegistry

    def __init__(self, bind: Engine):
        self._registry = _Registry()
        self._store = _Store(bind, self._registry)

    def get_bucket(self, model: Type[Base]) -> Optional[AbstractBucket]:
        try:
            return self._store.get_bucket(model)
        except KeyError:
            return None

    async def healthy(self) -> bool:
        return await self._store.healthy()

    async def create_bucket(self, model: Type[Base]) -> AbstractBucket:
        return await self._store.create_bucket(model)

    def get_buckets(self) -> list[AbstractBucket]:
        return self._store.get_buckets()

    async def delete_bucket(self, model: Type[Base]) -> bool:
        return await self._store.delete_bucket(model)

    def collect(self, base: Type[Base]) -> None:
        self._registry._autoload(base)