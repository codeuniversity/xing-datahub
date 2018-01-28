from confluent_kafka import Consumer, KafkaError
import sys
from build import Protocol_pb2

c = Consumer({'bootstrap.servers': 'localhost:9092', 'group.id': 'consumer',
              'default.topic.config': {'auto.offset.reset': 'smallest'}})
c.subscribe(['users', 'connections'])

def handle_message(msg):
    if msg.topic() == 'users':
        user = Protocol_pb2.User()
        user.ParseFromString(msg.value())
        print('User ------>')
        print(user)
    elif msg.topic() == 'connections':
        connection = Protocol_pb2.Connection()
        connection.ParseFromString(msg.value())
        print('Connection ----->')
        print(connection)
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
