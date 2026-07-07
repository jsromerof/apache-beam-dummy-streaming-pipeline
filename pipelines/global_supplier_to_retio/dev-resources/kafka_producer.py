import json
import os

from confluent_kafka import Producer

# Configure the producer
local_conf = {
    'bootstrap.servers': 'localhost:9092'
}

cloud_conf = {
    "bootstrap.servers": "pkc-lgk0v.us-west1.gcp.confluent.cloud:9092",
    "sasl.jaas.config": 'org.apache.kafka.common.security.plain.PlainLoginModule required serviceName="Kafka" username="" password="";', 
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "PLAIN"
}

p = Producer(local_conf)

def delivery_report(err, msg):
    """ Called once for each message transmitted to provide delivery results. """
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

TOPIC="leyland_topic"
# Produce a message
test_message = {
    "topic" : TOPIC,
    "source": "australia",
    "supplier_id":   "1",
    "supplier_name": "supplier_1",
    "timestamp": "2023-10-01T12:00:00.003Z",
    "supplier_data": [
        {
            "vendor_id":           "424216",
            "vendor_name":         "JOPP INTERIOR GMBH",
            "leyland_vendor_code": "H16457",
            "DAF_vendor_code":     "99123",
            "creation_date":       "2013-04-08T11:28:43",
            "last_update_date":    "2019-09-10T13:23:14",
            "address": {
                "street1":     "Am Jopp 1",
                "street2":     "Additional Street Info",
                "street3":     "More Street Info",
                "postal_code": "58566",
                "city":        "Kierspe",
                "country":     "Germany"
            },
            "timestamp": "2023-10-01T12:00:00.000Z"
        }
    ]
}

p.produce(TOPIC, json.dumps(test_message), callback=delivery_report)

# Wait for any outstanding messages to be delivered
p.flush()