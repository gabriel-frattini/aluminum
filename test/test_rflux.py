from rflux import RFlux
import asyncio
from dotenv import load_dotenv
import os
import pytest

load_dotenv(".env.local")
token = os.getenv("TOKEN") or ""

rflux = RFlux(
    host="http://localhost:8086",
    token=token,
    org="my-org",
)


@pytest.mark.asyncio
async def test_healthy_conn():
    ready = await rflux.healthy()
    assert ready


@pytest.mark.asyncio
async def test_incorrect_host():
    bad_rflux = RFlux(
        host="http://localhost:8085",
        token=token,
        org="my-org",
    )
    with pytest.raises(ConnectionError):
        await bad_rflux.healthy()


@pytest.mark.asyncio
async def test_write_with_bad_token():
    bad_rflux = RFlux(
        host="http://localhost:8086",
        token="bad-token",
        org="my-org",
    )

    with pytest.raises(ConnectionError):
        await bad_rflux.write(
            bucket="my-bucket",
            measurement="test measurement",
            tag="test tag",
            field="test field",
        )


@pytest.mark.asyncio
async def test_write_with_bad_organization():
    bad_rflux = RFlux(
        host="http://localhost:8086",
        token=token,
        org="bad-org",
    )

    with pytest.raises(ConnectionError):
        await bad_rflux.write(
            bucket="my-bucket",
            measurement="test measurement",
            tag="test tag",
            field="test field",
        )


@pytest.mark.asyncio
async def test_write_point():
    wrote = await rflux.write(
        bucket="my-bucket",
        measurement="test measurement",
        tag="test tag",
        field="test field",
    )
    assert wrote
