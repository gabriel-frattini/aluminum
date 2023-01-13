from typing import Optional, Type, List, Dict, Any

class DB:
    """

    Base class that contains all the methods to interact with the database.

    :param host: The host to connect to
    :param token: The token to use for authentication
    :param org: The organization to use

    """

    async def ping(self) -> bool:
        """
        Ping the influxdb server

        :return: True if the server is up and running, False otherwise
        """
    async def write(
        self,
        bucket: str,
        measurement: str,
        field: str,
        tag: str | None,
        timestamp: int | None,
    ) -> bool:
        """
        Write data to influxdb

        :param bucket: The bucket to write to
        :param measurement: The measurement to write to
        :param field: The field to write
        :param tag: The tag to write
        :param timestamp: The timestamp to write
        :return: True if the write was successful, False otherwise
        """
