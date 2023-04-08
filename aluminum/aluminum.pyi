from typing import Any, Type, TypeVar

from aluminum.abstract import (
    AbstractBase,
    AbstractBucket,
    AbstractMapped,
    AbstractRegistry,
    AbstractSelect,
    AbstractStore,
    AbstractWhereClause,
)
from aluminum.engine import Engine
from aluminum.base import Base
from aluminum.operator import WhereOperator

class _Bucket(AbstractBucket):
    """
    _Bucket is a class that represents a bucket of models in the database.

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
    async def query(self, select: AbstractSelect) -> list[Base]:
        """
        Queries the bucket using the given select instance.

        :param select: the select instance to use for querying
        """
    async def raw_query(self, select: str) -> list[Base]:
        """
        Queries the bucket using the given raw query.

        :param select: the raw query to use for querying
        """

class _Store(AbstractStore):
    """
    RFlux is the main class that contains all the methods to interact with the
    database.
    """

    def __new__(cls, bind: Engine, registry: AbstractRegistry):
        """

        Base class that contains all the methods to interact with the database.

        :param bind: the Engine instance to bind to
        """
    async def healthy(self) -> bool:
        """
        Check if the connection is healthy

        :return: True if the connection is healthy, False otherwise
        """
    async def create_bucket(self, model: Type[Base]) -> AbstractBucket:
        """
        Creates a new bucket for the given model supplied

        :param model: the Model schema to be used for this bucket

        :return: the new bucket
        """
    def get_bucket(self, model: Type[Base]) -> AbstractBucket:
        """
        Retrieves a bucket instance for the given model.

        :param model: the Model schema whose bucket is to be retrieved
        :return: the bucket instance
        """
    def get_buckets(self) -> list[AbstractBucket]:
        """
        Retrieves all the buckets.

        :return: a list of all the buckets
        """
    async def delete_bucket(self, model: Type[Base]) -> bool:
        """
        Deletes the bucket for the given model.

        :param model: the Model schema whose bucket is to be deleted
        """

class _Registry(AbstractRegistry):
    """
    _Registry is a class that contains all the metadata for the models.
    """

    def _autoload(self, base: Type[Base]) -> None:
        """
        Fills the metadata for the given model.

        :param model: the Model schema whose metadata is to be filled
        """

def get_schema(cls: Type[Base]) -> dict:
    """
    Returns the schema for the given model.

    :param cls: the Model schema whose schema is to be retrieved
    :return: the schema for the model
    """

class _Mapped(AbstractMapped):
    """
    _Mapped is a class that represents a mapped column in the database.
    """

    def __new__(cls, col_name: str):
        """
        Creates a new instance of _Mapped.
        """
    def _get_col_name(self) -> str:
        """
        Returns the column name.

        :return: the column name
        """

class _WhereClause(AbstractWhereClause):
    """
    _WhereClause is a class that represents a where clause in the database.

    """

    def __new__(
        cls, left_operand: _Mapped, operator: WhereOperator, right_operand: str
    ):
        """
        Creates a new instance of _WhereClause.
        """
        ...
    def __str__(self): ...
    def get_left_operand(self) -> _Mapped:
        """
        Returns the left operand of the where clause.

        :return: the left operand of the where clause
        """
    def get_operator(self) -> WhereOperator:
        """
        Returns the operator of the where clause.

        :return: the operator of the where clause
        """
    def get_right_operand(self) -> str:
        """
        Returns the right operand of the where clause.

        :return: the right operand of the where clause
        """
    def get_operator_str(self) -> str:
        """
        Returns the string representation of the operator.

        :return: the string representation of the operator
        """

class _Select(AbstractSelect):
    """
    _Select is a class that represents a select clause in the database.
    """

    def __init__(self, select: AbstractBase):
        """
        Creates a new instance of _Select.
        """
    def where(self, *args: AbstractWhereClause[Any]) -> AbstractSelect: 
    """
    Adds a where clause to the select clause.

    :param args: the where clauses to add
    :return: the select clause
    """
    ...
    def _where(
        self,
        left_operand: AbstractMapped[Any],
        operator: WhereOperator,
        right_operand: Any,
    ) -> AbstractSelect: 
    """
    Adds a where clause to the select clause.

    :param left_operand: the left operand of the where clause
    :param operator: the operator of the where clause
    :param right_operand: the right operand of the where clause
    :return: the select clause
    """
    ...
    def _create_range_str(self) -> None: 
    """
    Creates the range string for the select clause.
    """
    ...
    def _create_filter_str(self) -> None: 
    """
    Creates the filter string for the select clause.
    """
    ...
    def _create_bucket_str(self, name: str) -> None: 
    """
    Creates the bucket string for the select clause.
    """
    ...
    def _create_raw_query(self) -> str:
    """
    Creates the raw query for the select clause.
    """
    ...
    def _get_raw_query(self) -> str:
    """
    Returns the raw query for the select clause.
    """
    ...
