import logging
import os

import pytest
import pytest_asyncio
from dotenv import load_dotenv

from aluminum import Base, Store, create_engine
from aluminum.mapped_column import Mapped, mapped_column

logger = logging.getLogger(__name__)

load_dotenv(".env.local")
token = os.getenv("TOKEN") or ""
org_id = os.getenv("ORG_ID") or ""


class MockBucket(Base):
    measurement: Mapped[int] = mapped_column("measurement")
    tag: Mapped[str] = mapped_column("tag")
    field: Mapped[int] = mapped_column("field")


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


@pytest_asyncio.fixture(scope="function", autouse=True)
async def delete_mock_bucket(store: Store):
    yield
    logger.info("Deleting mock bucket")
    await store.delete_bucket(MockBucket)
