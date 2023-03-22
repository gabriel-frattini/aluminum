import asyncio
from rflux import RFlux, Base
from dotenv import load_dotenv
import os
import pytest
from rflux.Model import create_engine

from test.conftest import MockBucket, token


@pytest.mark.asyncio
async def test_healthy_conn(orm):
    ready = await orm.healthy()
    assert ready


@pytest.mark.asyncio
async def test_bad_engine():
    bad_engine = create_engine(
        host="http://localhost:1337",
        token=token,
        org_id="7e1e96f08517702b",
    )
    rflux = RFlux(bind=bad_engine)
    with pytest.raises(ConnectionError):
        await rflux.healthy()


@pytest.mark.asyncio
async def test_get_bucket(orm: RFlux):
    bucket = orm.get_bucket(MockBucket)
    assert bucket
    assert bucket.to_dict() == {
        "name": "MockBucket",
        "meta": {
            "schema": {
                "field": {"type": "string"},
                "measurement": {"type": "string"},
                "tag": {"type": "string"},
            }
        },
    }
