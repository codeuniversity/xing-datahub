from confluent_kafka import Consumer, KafkaError
import sys
# the mock-0.3.1 dir contains testcase.py, testutils.py & mock.py
from build import user_pb2

c = Consumer({'bootstrap.servers': 'localhost:9092', 'group.id': 'bla',
              'default.topic.config': {'auto.offset.reset': 'smallest'}})
c.subscribe(['users'])

running = True
while running:
    msg = c.poll(2)
    if msg is None:
        pass
    elif not msg.error():
        user = user_pb2.User()
        user.ParseFromString(msg.value())
        print('------>')
        print(user)
    elif msg.error().code() != KafkaError._PARTITION_EOF:
        print(msg.error())
        running = False
c.close()
