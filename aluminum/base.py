"""Module for the Model class."""

from pydantic import BaseModel


class _Mapper:
    _collected_buckets: dict[str, dict[str, list[BaseModel]]] = {
        "properties": {"buckets": []}
    }

    def _collect_bucket(self, bucket):
        self._collected_buckets["properties"]["buckets"].append(bucket)


class Base(BaseModel):
    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        _Mapper()._collect_bucket(cls)

    @staticmethod
    def _get_collected_buckets():
        return _Mapper()._collected_buckets
