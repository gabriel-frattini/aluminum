from typing import Optional, Self, Type, List, Dict, Any

class RFlux:
    def __new__(self, host: str, token: str, org: str):
        """

        Base class that contains all the methods to interact with the database.

        :param host: The host to connect to
        :param token: The token to use for authentication
        :param org: The organization to use

        """
    async def healthy(self) -> bool:
        """
        Check if the connection is healthy

        :return: True if the connection is healthy, False otherwise
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
