import pytest
from adeline import create_engine
from adeline import Session

from test.conftest import MockBucket, token


@pytest.mark.asyncio
async def test_healthy_conn(session):
    ready = await session.healthy()
    assert ready


@pytest.mark.asyncio
async def test_bad_engine():
    bad_engine = create_engine(
        host="http://localhost:1337",
        token=token,
        org_id="7e1e96f08517702b",
    )
    session = Session(bind=bad_engine)
    with pytest.raises(ConnectionError):
        await session.healthy()


@pytest.mark.asyncio
async def test_get_bucket(session: Session):
    bucket = session.get_bucket(MockBucket)
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
async def test_get_buckets(session: Session):
    class MockBucket2(MockBucket):
        measurement: str
        tag: str
        field: str

    await session.create_bucket(MockBucket2)

    buckets = session.get_buckets()
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
    await session.delete_bucket(MockBucket2)


@pytest.mark.asyncio
async def test_create_bucket(session: Session):
    await session.create_bucket(MockBucket)
    assert session.get_bucket(MockBucket)


@pytest.mark.asyncio
async def test_add_measurement(session: Session):
    await session.create_bucket(MockBucket)
    bucket = session.get_bucket(MockBucket)
    assert bucket
    measurement = MockBucket(
        measurement="measurement 7",
        tag="tag 7",
        field="field 7",
    )

    await bucket.add(measurement)


@pytest.mark.asyncio
async def test_delete_bucket(session: Session):
    class DeleteMockBucket(MockBucket):
        measurement: str
        tag: str
        field: str

    await session.create_bucket(DeleteMockBucket)
    await session.delete_bucket(DeleteMockBucket)

    assert not session.get_bucket(DeleteMockBucket)
