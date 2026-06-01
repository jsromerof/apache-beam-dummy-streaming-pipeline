from confluent_kafka import Producer

# Configure the producer
p = Producer({'bootstrap.servers': 'localhost:9092'})

def delivery_report(err, msg):
    """ Called once for each message transmitted to provide delivery results. """
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

# Produce a message
p.produce('input_topic', 
        """{
            "supplier_id" : "1", 
            "supplier_number":"number_1", 
            "supplier_name":"name_1", 
            "is_locked":false, 
            "load_date_time":"2023-01-01T00:00:00Z", 
            "update_date_time":"2023-01-01T00:00:10Z"
        }""", 
        callback=delivery_report)

# Wait for any outstanding messages to be delivered
p.flush()