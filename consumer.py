from confluent_kafka import Consumer, KafkaError
import sys
from build import Protocol_pb2
import hive_handler
from csv_export_handler import ExportHandler
c = Consumer({'bootstrap.servers': 'localhost:9092', 'group.id': 'consumer',
              'default.topic.config': {'auto.offset.reset': 'smallest'}})
c.subscribe(['users', 'connections'])
exporter = ExportHandler()

def handle_user(msg):
    user = Protocol_pb2.User()
    user.ParseFromString(msg.value())
    exporter.add_user(user)

def handle_connection(msg):
    connection = Protocol_pb2.Connection()
    connection.ParseFromString(msg.value())


def handle_message(msg):
    if msg.topic() == 'users':
        handle_user(msg)
    elif msg.topic() == 'connections':
        handle_connection(msg)
    else:
        print(msg.topic(), ' not handled')

running = True
while running:
    msg = c.poll(2)

    if msg is None:
        exporter.commit() # use poll timeout for hive-insertion
    elif not msg.error():
        handle_message(msg)
    elif msg.error().code() == KafkaError._PARTITION_EOF:
        exporter.commit() # when at end of topic insert prematurely into hive
    else:
        print(msg.error())
        running = False
c.close()
