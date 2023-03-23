import os
from dotenv import load_dotenv
import pytest
from adeline import Base, create_engine
import logging
import asyncio
from adeline import Session

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
def session():
    engine = create_engine(
        host="http://localhost:8086",
        token=token,
        org_id=org_id,
    )
    session = Session(bind=engine)
    session.collect(Base)
    yield session


# There doesn't seem to be a better way to run an async fixture after each test
async def delete_mock_bucket(session: Session):
    logger.info("Deleting mock bucket")
    await session.delete_bucket(MockBucket)


@pytest.fixture(scope="function", autouse=True)
def teardown(session):
    yield
    asyncio.run(delete_mock_bucket(session))
