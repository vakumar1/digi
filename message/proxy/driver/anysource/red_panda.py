import time
from kafka import KafkaProducer
from kafka.errors import KafkaError

def update_red_panda_broker_forever(brokers, topic):
    print("Writing to Redpanda broker forever...")
    data = 0
    while True:
        data += 1
        print(f"Writing to Redpanda broker {brokers} topic {topic}: data={data}", flush=True)
        try:
            producer = KafkaProducer(bootstrap_servers=brokers)
            message = str.encode(f"Proxy data: {data}")
            producer.send(topic, message)
            producer.flush()
        except KafkaError as e:
            print(f"Kafka Exception occurred: e={e}")
        time.sleep(10)
