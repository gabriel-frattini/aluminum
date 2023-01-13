from typing import Optional, Type, List, Dict, Any

class DB:
    """

    Base class that contains all the methods to interact with the database.

    :param host: The host to connect to
    :param token: The token to use for authentication
    :param org: The organization to use

    """

    def ping(self) -> bool:
        """
        Ping the influxdb server

        :return: True if the server is up and running, False otherwise
        """
