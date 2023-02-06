from rflux import RFlux, Model
import asyncio
from dotenv import load_dotenv
import os
import pytest

load_dotenv(".env.local")
token = os.getenv("TOKEN") or ""


def get_rflux_instance():
    return RFlux(
        host="http://localhost:8086",
        token=token,
        org="my-org",
    )


class Bucket(Model):
    measurement: str
    tag: str
    field: str


@pytest.mark.asyncio
async def test_healthy_conn():
    rflux = get_rflux_instance()
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
async def test_create_bucket():
    rflux = get_rflux_instance()
    bucket = rflux.create_bucket(Bucket)
    assert bucket


@pytest.mark.asyncio
async def test_get_bucket():
    rflux = get_rflux_instance()
    rflux.create_bucket(Bucket)
    bucket = rflux.get_bucket(Bucket)

    assert bucket


@pytest.mark.asyncio
async def test_insert_to_bucket():
    rflux = get_rflux_instance()
    rflux.create_bucket(Bucket)
    bucket = rflux.get_bucket(Bucket)
    measurement = Bucket(
        measurement="measurement test",
        tag="tag test",
        field="field test",
    )
    inserted = await bucket.insert(measurement)

    assert inserted
