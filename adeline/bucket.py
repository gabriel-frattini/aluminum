from adeline.adeline import _Bucket
from adeline.mapper import Base


class Bucket:

    _bucket: _Bucket

    def __init__(self, bucket: _Bucket):
        self._bucket = bucket

    async def add(self, item: Base) -> None:
        return await self._bucket.add(item)

    def to_dict(self) -> dict:
        return self._bucket.to_dict()
