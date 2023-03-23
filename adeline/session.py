from typing import Optional, Type
from adeline.bucket import Bucket
from adeline.mapper import Base
from adeline.engine import Engine
from adeline.adeline import _Registry, _Bucket


class Session:

    _store: _Registry

    def __init__(self, bind: Engine):
        self._store = _Registry(bind)

    def get_bucket(self, model: Type[Base]) -> Optional[_Bucket]:
        try:
            return self._store.get_bucket(model)
        except KeyError:
            return None

    async def healthy(self) -> bool:
        return await self._store.healthy()

    async def create_bucket(self, model: Type[Base]) -> Bucket:
        return await self._store.create_bucket(model)

    def get_buckets(self) -> list[_Bucket]:
        return self._store.get_buckets()

    async def delete_bucket(self, model: Type[Base]) -> bool:
        return await self._store.delete_bucket(model)

    def collect(self, base: Type[Base]) -> None:
        self._store._autoload(base)
