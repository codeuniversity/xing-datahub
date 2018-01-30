from confluent_kafka import Consumer, KafkaError
import sys
from build import Protocol_pb2
import hive_handler

c = Consumer({'bootstrap.servers': 'localhost:9092', 'group.id': 'consumer',
              'default.topic.config': {'auto.offset.reset': 'smallest'}})
c.subscribe(['users', 'connections'])
batch_size = 10
user_batch = []
def handle_user(msg):
    user = Protocol_pb2.User()
    user.ParseFromString(msg.value())
    user_batch.append(user)
    if len(user_batch) >= batch_size:
        hive_handler.insert_users(user_batch)
    # hive_handler.insert_user(user)
def handle_connection(msg):
    connection = Protocol_pb2.Connection()
    connection.ParseFromString(msg.value())


def handle_message(msg):
    if msg.topic() == 'users':
        handle_user(msg)
    elif msg.topic() == 'connections':
        handle_connection(msg)
    else:
        print(msg.topic())
running = True
while running:
    msg = c.poll(2)

    if msg is None:
        pass
    elif not msg.error():
        handle_message(msg)
    elif msg.error().code() != KafkaError._PARTITION_EOF:
        print(msg.error())
        running = False
c.close()
