import pytest
from aluminum import create_engine
from aluminum import Store
from aluminum.base import Base

from test.conftest import MockBucket, token


@pytest.mark.asyncio
async def test_healthy_conn(store):
    ready = await store.healthy()
    assert ready


@pytest.mark.asyncio
async def test_bad_engine():
    bad_engine = create_engine(
        host="http://localhost:1337",
        token=token,
        org_id="7e1e96f08517702b",
    )
    store = Store(bind=bad_engine)
    with pytest.raises(ConnectionError):
        await store.healthy()


@pytest.mark.asyncio
async def test_get_buckets(store: Store):
    await store.delete_bucket(MockBucket)

    class MockBucket2(Base):
        measurement: str
        tag: str
        field: str

    await store.create_bucket(MockBucket)
    await store.create_bucket(MockBucket2)

    buckets = store.get_buckets()
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
    await store.delete_bucket(MockBucket2)


@pytest.mark.asyncio
async def test_create_bucket(store: Store):
    await store.create_bucket(MockBucket)
    bucket = store.get_bucket(MockBucket)
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
async def test_add_measurement(store: Store):
    await store.create_bucket(MockBucket)
    bucket = store.get_bucket(MockBucket)
    assert bucket
    measurement = MockBucket(
        measurement="test measurement",
        tag="test tag",
        field="test field",
    )

    await bucket.add(measurement)


@pytest.mark.asyncio
async def test_delete_bucket(store: Store):
    class DeleteMockBucket(MockBucket):
        measurement: str
        tag: str
        field: str

    await store.create_bucket(DeleteMockBucket)
    await store.delete_bucket(DeleteMockBucket)

    assert not store.get_bucket(DeleteMockBucket)