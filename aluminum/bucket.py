from aluminum.abstract import AbstractBucket, AbstractSelect
from aluminum.base import Base
from aluminum.result import Result
from aluminum.select import Select


class Bucket(AbstractBucket):

    _bucket: AbstractBucket

    def __init__(self, bucket: AbstractBucket):
        self._bucket = bucket

    async def add(self, item: Base) -> None:
        return await self._bucket.add(item)

    def to_dict(self) -> dict:
        return self._bucket.to_dict()

    async def query(self, select: AbstractSelect) -> list[Base]:
        return await self._bucket.query(select)

    async def raw_query(self, select: str) -> list[Base]:
        result = await self._bucket.raw_query(select)
        name = result["name"]
        query_data = result["data"]
        cached_buckets = Base._get_collected_buckets()["buckets"]
        # TODO
        BucketClass = [bucket for bucket in cached_buckets if bucket.__name__ == name][
            0
        ]
        return [BucketClass(**d) for d in query_data]

    async def execute(self, select: Select) -> list[Base]:
        select._create_raw_query()
        query = select._get_raw_query()
        result = await self._bucket.raw_query(query)
        name = result["name"]
        query_data = result["data"]
        cached_buckets = Base._get_collected_buckets()["buckets"]
        BucketClass = [bucket for bucket in cached_buckets if bucket.__name__ == name][
            0
        ]
        return [BucketClass(**d) for d in query_data]
