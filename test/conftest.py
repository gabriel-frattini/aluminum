import os
from dotenv import load_dotenv
import pytest
from rflux.Model import Base, create_engine
import logging
import asyncio
from rflux import FluxSession

logger = logging.getLogger(__name__)

load_dotenv(".env.local")
token = os.getenv("TOKEN") or ""
org_id = os.getenv("ORG_ID") or ""


class MockBucket(Base):
    measurement: str
    tag: str
    field: str


@pytest.fixture(scope="function", autouse=True)
@pytest.mark.asyncio
def orm():
    engine = create_engine(
        host="http://localhost:8086",
        token=token,
        org_id=org_id,
    )
    rflux = FluxSession(bind=engine)
    rflux.collect(Base)
    yield rflux


# There doesn't seem to be a better way to run an async fixture after each test
async def delete_mock_bucket(orm: FluxSession):
    logger.info("Deleting mock bucket")
    await orm.delete_bucket(MockBucket)


@pytest.fixture(scope="function", autouse=True)
def teardown(orm):
    yield
    asyncio.run(delete_mock_bucket(orm))
