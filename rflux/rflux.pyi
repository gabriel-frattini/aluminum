from typing import Type

from rflux.Model import Model

class RFluxBucket:
    """
    RFluxbucket is a class that represents a bucket of models in the database.
    It contains methods to interact with the bucket.
    """

    async def insert(self, item: Model) -> None:
        """
        Adds a single Model instance to the bucket. The model instance should
        be of the same type as the model used to get this bucket.

        :param item: the model object to insert
        """

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
    def create_bucket(self, model: Type[Model]) -> Type[Model]:
        """
        Creates a new bucket for the given model supplied

        :param model: the Model schema to be used for this bucket

        :return: the new bucket
        """
    def get_bucket(self, model: Type[Model]) -> RFluxBucket:
        """
        Retrieves a bucket instance for the given model

        :param model: the Model schema whose bucket is to be retrieved
        :return: the bucket instance
        """
