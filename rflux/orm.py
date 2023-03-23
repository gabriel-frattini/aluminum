from typing import Optional, Type
from rflux.Model import Base, Engine
from rflux.rflux import RFlux, RFluxBucket


class BucketInstance:

    _bucket: RFluxBucket

    def __init__(self, bucket: RFluxBucket):
        self._bucket = bucket

    async def add(self, item: Base) -> None:
        return await self._bucket.add(item)

    def to_dict(self) -> dict:
        return self._bucket.to_dict()


class FluxSession:

    _store: RFlux

    def __init__(self, bind: Engine):
        self._store = RFlux(bind)

    def get_bucket(self, model: Type[Base]) -> Optional[RFluxBucket]:
        try:
            return self._store.get_bucket(model)
        except KeyError:
            return None

    async def healthy(self) -> bool:
        return await self._store.healthy()

    async def create_bucket(self, model: Type[Base]) -> Type[Base]:
        return await self._store.create_bucket(model)

    def get_buckets(self) -> list[RFluxBucket]:
        return self._store.get_buckets()

    async def delete_bucket(self, model: Type[Base]) -> bool:
        return await self._store.delete_bucket(model)

    def collect(self, base: Type[Base]) -> None:
        self._store._autoload(base)
