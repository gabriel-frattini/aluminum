from __future__ import annotations
from aluminum.abstract import AbstractBase


class _Mapper:
    _collected_buckets: dict[str, list[AbstractBase]] = {"buckets": []}

    def _collect_bucket(self, bucket):
        self._collected_buckets["buckets"].append(bucket)


class Base(AbstractBase):
    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        _Mapper()._collect_bucket(cls)

    @staticmethod
    def _get_collected_buckets():
        return _Mapper()._collected_buckets
