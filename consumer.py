from confluent_kafka import Consumer, KafkaError
import sys
from build import Protocol_pb2
import hive_handler
from csv_export_handler import ConnectionExportHandler, UserExportHandler, user_to_csv_line, connection_to_csv_line
c = Consumer({'bootstrap.servers': 'localhost:9092', 'group.id': 'consumer',
              'default.topic.config': {'auto.offset.reset': 'smallest'}})
c.subscribe(['users', 'connections'])
user_exporter = UserExportHandler(batch_size = 2500)
connection_exporter = ConnectionExportHandler(batch_size = 20000, name='connections', converter=connection_to_csv_line, schema_string=hive_handler.connection_schema_string)

def handle_user(msg):
    user = Protocol_pb2.User()
    user.ParseFromString(msg.value())
    user_exporter.add(user)

def handle_connection(msg):
    connection = Protocol_pb2.Connection()
    connection.ParseFromString(msg.value())
    connection_exporter.add(connection)

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
        # use poll timeout for hive-insertion
        user_exporter.commit()
        connection_exporter.commit()
    elif not msg.error():
        handle_message(msg)
    elif msg.error().code() == KafkaError._PARTITION_EOF:
        # when at end of topic insert prematurely into hive
        user_exporter.commit()
        connection_exporter.commit()
    else:
        print(msg.error())
        running = False
c.close()
