from kafka import KafkaProducer
import time


class Producer():
    def start(self):
        producer = KafkaProducer(bootstrap_servers='localhost:9092')

        for i in range(3):
            producer.send('data', 'test \#{}'.format(i).encode('utf-8'))
            print(i)
            time.sleep(0.5)

        producer.close()


Producer().start()
