from types import GenericAlias
from typing import Any

from aluminum.abstract import AbstractBase
from aluminum.aluminum import get_schema


class _Mapper:
    _collected_buckets: dict[str, list[AbstractBase]] = {"buckets": []}

    def _collect_bucket(self, bucket):
        self._collected_buckets["buckets"].append(bucket)


class Base(AbstractBase):
    def __init_subclass__(cls, **kwargs):
        def __init__(self, **kwargs):
            for key, value in kwargs.items():
                setattr(self, key, value)

        cls.__init__ = __init__

        super().__init_subclass__()
        _Mapper()._collect_bucket(cls)

    @staticmethod
    def _get_collected_buckets() -> dict[str, list[AbstractBase]]:
        return _Mapper()._collected_buckets

    @classmethod
    def schema(cls) -> dict[Any, Any]:
        return get_schema(cls)

    def dict(self):
        return self.__dict__
