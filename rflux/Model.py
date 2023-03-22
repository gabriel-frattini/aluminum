"""Module for the Model class."""

from pydantic import BaseModel


class Mapper:
    _collected_buckets: dict[str, dict[str, list[BaseModel]]] = {
        "properties": {"buckets": []}
    }

    def _collect_bucket(self, bucket):
        self._collected_buckets["properties"]["buckets"].append(bucket)


class Base(BaseModel):
    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        Mapper()._collect_bucket(cls)

    @staticmethod
    def _get_collected_buckets():
        return Mapper()._collected_buckets


class Engine:
    host: str
    token: str
    org_id: str

    def __init__(self, host: str, token: str, org_id: str) -> None:
        self.host = host
        self.token = token
        self.org_id = org_id


def create_engine(host: str, token: str, org_id: str) -> Engine:
    """
    Creates a new Engine instance

    :param host: The host to connect to
    :param token: The token to use for authentication
    :param org_id: The organization id to use

    :return: the new Engine instance
    """
    return Engine(host, token, org_id)
