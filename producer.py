from confluent_kafka import Producer
import json
import aiohttp
import asyncio
import async_timeout
import sys
# the mock-0.3.1 dir contains testcase.py, testutils.py & mock.py
sys.path.append('build')
from build import user_pb2

p = Producer({'bootstrap.servers': 'localhost:9092'})


def push_to_kafka(msg):
    user_dict_array = json.loads(msg)
    for user_dict in user_dict_array:
        user = user_pb2.User()
        user.id = user_dict['id']
        user.first_name = user_dict['first_name']
        user.last_name = user_dict['last_name']
        kafka_message = user.SerializeToString()
        p.produce('users', kafka_message)
    p.flush()


async def fetch(session, url):
    with async_timeout.timeout(10):
        async with session.get(url) as response:
            return await response.text()


async def main():
    async with aiohttp.ClientSession() as session:
        msg = await fetch(session, 'http://localhost:8080/users')
        push_to_kafka(msg)

loop = asyncio.get_event_loop()
loop.run_until_complete(main())
