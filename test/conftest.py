import os
from dotenv import load_dotenv
import pytest
from aluminum import Base, create_engine
import logging
import asyncio
from aluminum import Store

logger = logging.getLogger(__name__)

load_dotenv(".env.local")
token = os.getenv("TOKEN") or ""
org_id = os.getenv("ORG_ID") or ""


class MockBucket(Base):
    measurement: str
    tag: str
    field: int


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
async def delete_mock_bucket(store: Store):
    logger.info("Deleting mock bucket")
    await store.delete_bucket(MockBucket)


@pytest.fixture(scope="function", autouse=True)
def teardown(store):
    yield
    asyncio.run(delete_mock_bucket(store))
