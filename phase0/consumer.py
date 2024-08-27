from confluent_kafka import Consumer, KafkaException
from json import loads, JSONDecodeError  # Import JSONDecodeError

# Create a consumer configuration
conf = {
    'bootstrap.servers': 'localhost:9092',  # Kafka broker address
    'group.id': 'mygroup',  # Consumer group id
    'auto.offset.reset': 'earliest'  # Start from the beginning of the topic if no offset is found
}

# Create a Consumer instance
consumer = Consumer(conf)

# Subscribe to the topic
consumer.subscribe(['test'])

try:
    while True:
        msg = consumer.poll(1.0)  # Timeout in seconds
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaException._PARTITION_EOF:
                # End of partition event
                print(f'End of partition reached {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')
            elif msg.error():
                # Error happened
                raise KafkaException(msg.error())
        else:
            # Proper message received
            message_value = msg.value()
            if message_value:  # Check if message is not None or empty
                try:
                    message = loads(message_value.decode('ascii'))  # Deserialize JSON message
                    print(f"Received message: {message}")
                    print(f"{message} found")
                except JSONDecodeError as e:
                    print(f"Failed to decode JSON: {e}")
                    print(f"Message content: {message_value}")
            else:
                print("Received empty message.")
finally:
    # Close down consumer to commit final offsets.
    consumer.close()
