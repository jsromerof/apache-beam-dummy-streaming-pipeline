from confluent_kafka import Producer
import time

# Configure the producer
p = Producer({'bootstrap.servers': 'localhost:9092'})

def delivery_report(err, msg):
    """ Called once for each message transmitted to provide delivery results. """
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

unix_time = int(time.time())
print(unix_time)

message='{"chassis_id" : "1" , "chassis_number":"number_10"}'
print(message)
# Produce a message
p.produce(topic='input_topic', value=message, timestamp = unix_time, callback=delivery_report)

# Wait for any outstanding messages to be delivered
p.flush()