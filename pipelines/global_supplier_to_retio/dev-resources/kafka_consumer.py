from confluent_kafka import Consumer, KafkaException, KafkaError
import sys
import os


local_conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'consumer-group-1',
    'auto.offset.reset': 'earliest', # Start consuming from the beginning of the topic if no offset is stored
    'enable.auto.commit': True,
    'session.timeout.ms': 6000
}

cloud_conf = consumer_config_kafka_cloud = {
    "bootstrap.servers": f"{os.getenv('KAFKA_BOOTSTRAP_SERVERS')}",
    "group.id": "consumer-group-1",
    "sasl.jaas.config": f'org.apache.kafka.common.security.plain.PlainLoginModule required serviceName="Kafka" username="{os.getenv("KAFKA_USERNAME")}" password="{os.getenv("KAFKA_PASSWORD")}";',
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "SCRAM-SHA-512",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": "false"
}

TOPIC = 'australia_topic'  # Change this to your topic name

# Create a new Consumer instance
consumer = Consumer(local_conf)

try:
    # Subscribe to the topic
    consumer.subscribe([TOPIC])

    print(f"Consumer started. Subscribed to topic: {TOPIC}. Waiting for messages...")

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