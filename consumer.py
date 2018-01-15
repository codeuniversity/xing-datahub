from kafka import KafkaConsumer


class Consumer():
    def start(self):
        consumer = KafkaConsumer(bootstrap_servers='localhost:9092',
                                 auto_offset_reset='earliest',
                                 consumer_timeout_ms=1000)
        consumer.subscribe(['data'])
        while True:
            for message in consumer:
                print(message)

        consumer.close()

Consumer().start()
