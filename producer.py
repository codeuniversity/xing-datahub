from confluent_kafka import Producer
import json
from aiohttp import web
import asyncio
import async_timeout
import sys
# the mock-0.3.1 dir contains testcase.py, testutils.py & mock.py
from build import user_pb2
from google.protobuf import json_format

p = Producer({'bootstrap.servers': 'localhost:9092'})

def serialize_user(json_str):
    return json_format.Parse(json_str, user_pb2.User()).SerializeToString()

def push_to_kafka(msg):
    kafka_message = serialize_user(msg)
    print(kafka_message)
    p.produce('users', kafka_message)
    p.flush()


async def handle_users(request):
    user_str = await request.text()
    push_to_kafka(user_str)
    return web.Response()

app = web.Application()
app.router.add_post('/users', handle_users)

web.run_app(app)
