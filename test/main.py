from rflux.rflux import DB
import asyncio
from dotenv import load_dotenv
import os

load_dotenv(".env.local")
token = os.getenv("TOKEN")

db = DB(
    host="http://localhost:8086",
    token=token,
    org="my-org",
)


async def main():
    print(await db.ping())


asyncio.run(main())
