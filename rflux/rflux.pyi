from typing import Type

from rflux.Model import Base, Engine

class RFluxBucket:
    """
    RFluxbucket is a class that represents a bucket of models in the database.

    It contains all the methods to interact with the bucket.
    """

    async def add(self, item: Base) -> None:
        """
        Adds a single Model instance to the bucket. The model instance should
        be of the same type as the model used to get this bucket.

        :param item: the model object to add
        """
    def to_dict(self) -> dict:
        """
        Converts the bucket to a dictionary.

        :return: the dictionary representation of the bucket
        """

class RFlux:
    """
    RFlux is the main class that contains all the methods to interact with the
    database.
    """

    def __new__(cls, bind: Engine):
        """

        Base class that contains all the methods to interact with the database.

        :param bind: the Engine instance to bind to
        """
    async def healthy(self) -> bool:
        """
        Check if the connection is healthy

        :return: True if the connection is healthy, False otherwise
        """
    async def create_bucket(self, model: Type[Base]) -> Type[Base]:
        """
        Creates a new bucket for the given model supplied

        :param model: the Model schema to be used for this bucket

        :return: the new bucket
        """
    def get_bucket(self, model: Type[Base]) -> RFluxBucket:
        """
        Retrieves a bucket instance for the given model.

        :param model: the Model schema whose bucket is to be retrieved
        :return: the bucket instance
        """
    def get_buckets(self) -> list[RFluxBucket]:
        """
        Retrieves all the buckets.

        :return: a list of all the buckets
        """
    async def delete_bucket(self, model: Type[Base]) -> bool:
        """
        Deletes the bucket for the given model.

        :param model: the Model schema whose bucket is to be deleted
        """
    def _autoload(self, base: Type[Base]) -> None:
        """
        Fills the metadata for the given model.

        :param model: the Model schema whose metadata is to be filled
        """
