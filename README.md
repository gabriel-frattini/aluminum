
Aluminum
==========

A fast python ORM for InfluxDB 2 written in Rust.


Introduction
==========

Aluminum is a fast Python library written in Rust that provides an ORM interface for interacting with InfluxDB.


Getting Started
==========

This section will guide you through the basic steps of using the library. It will cover:

- Setting up a connection with an engine
- Creating a bucket
- Retrieving a bucket
- Adding data to a bucket
- Querying data from a bucket


#### Installation

you can install Aluminum using pip:

```sh
pip install aluminum
```

To use the library, you first need to create an instance of an engine and bind it to the Store.

```python
from aluminum import Engine, Store, Base

engine = create_engine(
        host="http://localhost:8086",
        token="<INFLUXDB-TOKEN>",
        org_id="<ORG-ID>",
    )

# Bind it to the store
store = Store(bind=engine)
# Initialize the Store's metadata for your models
store.collect(Base)
```

#### The Store

After setting up the store, you can create a bucket by calling the `create_bucket` method of the Store instance. 
The method takes a class that inherits from `Base` as an argument and returns a bucket instance. 

```python
from aluminum.base import Base

class SensorBucket(Base):
  tag: str
  measurement: str
  field: int

async def run_async_example():
  # Create a bucket
  bucket = await store.create_bucket(SensorBucket)
  
  # Get a bucket
  bucket = store.get_bucket(SensorBucket)
  
  # Get all buckets
  buckets = store.get_buckets()
  
  # Delete a bucket
  await store.delete_bucket(SensorBucket)
```

#### Adding Data to a Bucket

To add data to a bucket, you can call the `add` method of the bucket instance. The add method takes an instance of the bucket class as an argument.

```python
from aluminum.base import Base

class SensorBucket(Base):
  tag: str
  measurement: str
  field: int

async def run_async_example():
  msmnt = SensorBucket(tag="My Tag", measurement="My Measurement", field=10)
  await bucket.add(user)
```

#### Querying Data from a Bucket

To query data from a bucket, you can call the `execute` method of the bucket instance. The execute method takes a Select instance as an argument and returns a list of bucket class instances that match the query.

```python
from aluminum import select

async def run_async_example():
  stmt = select(SensorBucket).where(SensorBucket.tag == "My Tag", SensorBucket.field > 0)
  result = await bucket.execute(stmt) # list of SensorBucket
```

#### Acknowledgement

- The python-rust bindings are from [the pyo3 project](https://github.com/PyO3)

#### License

Licensed under the MIT License.

Copyright (c) 2022 [Gabriele Frattini](https://github.com/gabriel-frattini)

Need Help?

If you have any questions or need help getting started, please reach out by opening an issue.

Conributions are welcome.
