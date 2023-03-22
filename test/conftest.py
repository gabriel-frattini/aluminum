import os
from dotenv import load_dotenv
import pytest
from rflux.Model import Base, create_engine

from rflux.rflux import RFlux

load_dotenv(".env.local")
token = os.getenv("TOKEN") or ""


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
        org_id="7e1e96f08517702b",
    )
    rflux = RFlux(bind=engine)
    rflux.collect_all(Base)
    yield rflux
