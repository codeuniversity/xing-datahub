from confluent_kafka import Producer
import json
from aiohttp import web
import asyncio
import async_timeout
import sys
# the mock-0.3.1 dir contains testcase.py, testutils.py & mock.py
from build import Protocol_pb2
from google.protobuf import json_format

p = Producer({'bootstrap.servers': 'localhost:9092'})

def serialize_user(json_str):
    return json_format.Parse(json_str, Protocol_pb2.User()).SerializeToString()

def serialize_connection(json_str):
    return json_format.Parse(json_str, Protocol_pb2.Connection()).SerializeToString()

def push_user_to_kafka(msg):
    kafka_message = serialize_user(msg)
    p.produce('users', kafka_message)
    p.flush()

def push_connection_to_kafka(msg):
    kafka_message = serialize_connection(msg)
    p.produce('connections', kafka_message)
    p.flush()

async def handle_user(request):
    user_str = await request.text()
    push_user_to_kafka(user_str)
    return web.Response()

async def handle_connection(request):
    connection_str = await request.text()
    push_connection_to_kafka(connection_str)
    return web.Response()

app = web.Application()
app.router.add_post('/users', handle_user)
app.router.add_post('/connections', handle_connection)
web.run_app(app)
