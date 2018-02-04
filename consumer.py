from confluent_kafka import Consumer, KafkaError
import sys
from build import protocol_pb2
import hive_handler
from csv_export_handler import ExportHandler
c = Consumer({'bootstrap.servers': 'localhost:9092', 'group.id': 'written file consumer',
              'default.topic.config': {'auto.offset.reset': 'smallest'}})
c.subscribe(['written_files'])
user_exporter = ExportHandler()
connection_exporter = ExportHandler(name='connections', schema_string=hive_handler.connection_schema_string)

def handle_file(msg):
    csvInfo = protocol_pb2.WrittenCSVInfo()
    csvInfo.ParseFromString(msg.value())
    t = csvInfo.recordType
    if t == "users":
        user_exporter.commit(csvInfo.filepath, csvInfo.filename)
    elif t == "connections":
        connection_exporter.commit(csvInfo.filepath, csvInfo.filename)
    else:
        print("recod type ", t, " not supported")

def handle_connection(msg):
    connection = protocol_pb2.Connection()
    connection.ParseFromString(msg.value())
    connection_exporter.add(connection)

def handle_message(msg):
    if msg.topic() == 'written_files':
        handle_file(msg)
    else:
        print(msg.topic(), ' not handled')

running = True
while running:
    msg = c.poll(2)
    if msg is None:
        pass
    elif not msg.error():
        handle_message(msg)
    elif msg.error().code() == KafkaError._PARTITION_EOF:
        pass
    else:
        print(msg.error())
        running = False
c.close()
