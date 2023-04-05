from types import GenericAlias
from typing import Any

from attr import dataclass

from aluminum.abstract import AbstractBase


class _Mapper:
    _collected_buckets: dict[str, list[AbstractBase]] = {"buckets": []}

    def _collect_bucket(self, bucket):
        self._collected_buckets["buckets"].append(bucket)


@dataclass(init=True, kw_only=True)
class Base(AbstractBase):
    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        _Mapper()._collect_bucket(cls)

    @staticmethod
    def _get_collected_buckets():
        return _Mapper()._collected_buckets

    # TODO
    @classmethod
    def schema(cls) -> dict[Any, Any]:
        schema = {
            "title": cls.__name__,
            "type": "object",
            "properties": {},
            "required": [],
        }
        for key, val in cls.__annotations__.items():
            if type(val).__name__ == "_" + GenericAlias.__name__:
                schema["properties"][key] = {
                    "title": key,
                    "type": val.__args__[0].__name__,
                }
                schema["required"].append(key)
            else:
                schema["properties"][key] = {"title": key, "type": type(val).__name__}
                schema["required"].append(key)
        return schema

    def dict(self):
        return self.__dict__
