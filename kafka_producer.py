import json

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
test_message = {
    "topic" : "leyland_topic",
    "supplier_id":   "1",
    "supplier_name": "supplier_1",
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
            }
        },
        {
            "vendor_id":           "528242",
            "vendor_name":         "OHM HANER METALLWERK GMBH CO KG",
            "leyland_vendor_code": "003554",
            "DAF_vendor_code":     "87148",
            "creation_date":       "2014-10-24T13:01:59",
            "last_update_date":    "2019-09-19T15:41:47",
            "address": {
                "street1":     "Am Jopp 1",
                "street2":     "Additional Street Info",
                "street3":     "More Street Info",
                "postal_code": "58566",
                "city":        "Kierspe",
                "country":     "Germany"
            }
        },
        {
            "vendor_id":           "528243",
            "vendor_name":         "PETER GFK SPOL SRO",
            "leyland_vendor_code": "P17032",
            "DAF_vendor_code":     "86810",
            "creation_date":       "2014-10-24T13:08:06",
            "last_update_date":    "2019-09-19T15:41:47",
            "address": {
                "street1":     "ul. PETER 1",
                "street2":     "Additional Street Info",
                "street3":     "More Street Info",
                "postal_code": "58566",
                "city":        "Kierspe",
                "country":     "Germany"
            }
        }
    ]
}
p.produce("leyland_topic", json.dumps(test_message), callback=delivery_report)

# Wait for any outstanding messages to be delivered
p.flush()