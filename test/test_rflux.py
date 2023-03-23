import pytest
from rflux.Model import create_engine
from rflux.orm import FluxSession

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
    rflux = FluxSession(bind=bad_engine)
    with pytest.raises(ConnectionError):
        await rflux.healthy()


@pytest.mark.asyncio
async def test_get_bucket(orm: FluxSession):
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


@pytest.mark.asyncio
async def test_get_buckets(orm: FluxSession):
    class MockBucket2(MockBucket):
        measurement: str
        tag: str
        field: str

    await orm.create_bucket(MockBucket2)

    buckets = orm.get_buckets()
    assert buckets
    assert sorted(
        [bucket.to_dict() for bucket in buckets], key=lambda x: x["name"]
    ) == [
        {
            "name": "MockBucket",
            "meta": {
                "schema": {
                    "field": {"type": "string"},
                    "measurement": {"type": "string"},
                    "tag": {"type": "string"},
                }
            },
        },
        {
            "name": "MockBucket2",
            "meta": {
                "schema": {
                    "field": {"type": "string"},
                    "measurement": {"type": "string"},
                    "tag": {"type": "string"},
                }
            },
        },
    ]


@pytest.mark.asyncio
async def test_create_bucket(orm: FluxSession):
    await orm.create_bucket(MockBucket)
    assert orm.get_bucket(MockBucket)


@pytest.mark.asyncio
async def test_add_measurement(orm: FluxSession):
    await orm.create_bucket(MockBucket)
    bucket = orm.get_bucket(MockBucket)
    assert bucket
    measurement = MockBucket(
        measurement="measurement 7",
        tag="tag 7",
        field="field 7",
    )

    await bucket.add(measurement)


@pytest.mark.asyncio
async def test_delete_bucket(orm: FluxSession):
    class DeleteMockBucket(MockBucket):
        measurement: str
        tag: str
        field: str

    await orm.create_bucket(DeleteMockBucket)
    await orm.delete_bucket(DeleteMockBucket)

    assert not orm.get_bucket(DeleteMockBucket)
