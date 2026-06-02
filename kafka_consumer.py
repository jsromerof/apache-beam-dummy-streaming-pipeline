from confluent_kafka import Consumer, KafkaException, KafkaError
import sys


conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'consumer-group-1',
    'auto.offset.reset': 'earliest', # Start consuming from the beginning of the topic if no offset is stored
    'enable.auto.commit': True,
    'session.timeout.ms': 6000
}

topic = 'leyland_topic'

# Create a new Consumer instance
consumer = Consumer(conf)

try:
    # Subscribe to the topic
    consumer.subscribe([topic])

    print(f"Consumer started. Subscribed to topic: {topic}. Waiting for messages...")

    while True:
        # Poll for messages with a timeout of 1.0 second
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event - not an error, just an indication
                sys.stderr.write(
                    '%% %s [%d] reached end of offset %d\n' %
                    (msg.topic(), msg.partition(), msg.offset())
                )
            elif msg.error():
                # Other error
                raise KafkaException(msg.error())
        else:
            # Proper message received
            print(f"Received message: key={msg.key()}, value={msg.value()}, topic={msg.topic()}, partition={msg.partition()}, offset={msg.offset()}")

except KeyboardInterrupt:
    sys.stderr.write('%% Aborted by user\n')

finally:
    # Close down the consumer to commit final offsets and terminate the session
    consumer.close()
    print("Consumer closed.")