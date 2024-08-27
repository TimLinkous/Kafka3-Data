from time import sleep
from json import dumps
from confluent_kafka import Producer

# Create a producer configuration
conf = {
    'bootstrap.servers': 'localhost:9092'  # Kafka broker address
}

# Create a Producer instance
producer = Producer(conf)

# Delivery report callback function
def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

# Produce messages
for e in range(1000):
    data = {'number': e}
    print(data)
    # Serialize the message data to JSON
    producer.produce('test', key=None, value=dumps(data), callback=delivery_report)
    producer.poll(0)  # Serve delivery reports (non-blocking)
    sleep(5)

# Wait for any outstanding messages to be delivered and delivery reports received
producer.flush()
