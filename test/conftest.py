import os
from attr import dataclass
from dotenv import load_dotenv
import pytest
from aluminum import Base, create_engine
import logging
import asyncio
from aluminum import Store
import pytest_asyncio

from aluminum.mapped_column import MappedColumn, mapped_column

logger = logging.getLogger(__name__)

load_dotenv(".env.local")
token = os.getenv("TOKEN") or ""
org_id = os.getenv("ORG_ID") or ""


@dataclass(kw_only=True)
class MockBucket(Base):
    measurement: MappedColumn[int] = mapped_column("measurement")
    tag: MappedColumn[str] = mapped_column("tag")
    field: MappedColumn[int] = mapped_column("field")


@pytest.fixture(scope="function", autouse=True)
@pytest.mark.asyncio
def store():
    engine = create_engine(
        host="http://localhost:8086",
        token=token,
        org_id=org_id,
    )
    store = Store(bind=engine)
    store.collect(Base)
    yield store


# There doesn't seem to be a better way to run an async fixture after each test
@pytest_asyncio.fixture(scope="function", autouse=True)
async def delete_mock_bucket(store: Store):
    yield
    logger.info("Deleting mock bucket")
    await store.delete_bucket(MockBucket)
