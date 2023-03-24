from aluminum.abstract import AbstractBucket
from aluminum.base import Base


class Bucket:

    _bucket: AbstractBucket

    def __init__(self, bucket: AbstractBucket):
        self._bucket = bucket

    async def add(self, item: Base) -> None:
        return await self._bucket.add(item)

    def to_dict(self) -> dict:
        return self._bucket.to_dict()
