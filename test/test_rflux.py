from rflux import DB
import asyncio
from dotenv import load_dotenv
import os
import pytest

load_dotenv(".env.local")
token = os.getenv("TOKEN")

db = DB(
    host="http://localhost:8086",
    token=token,
    org="my-org",
)


@pytest.mark.asyncio
async def test_db_ping():
    ready = await db.ping()
    assert ready


@pytest.mark.asyncio
async def test_write_point():
    wrote = await db.write(
        bucket="my-bucket",
        measurement="test",
        tag="test",
        field="test",
        timestamp=123456789,
    )
    assert wrote
