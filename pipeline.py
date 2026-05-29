import apache_beam as beam
from apache_beam.io.kafka import ReadFromKafka, WriteToKafka
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import json
import yaml
from src.schema_standardizer import SchemaStandardizer

class DecodeMessage(beam.DoFn):
    
    def process(self, element):
        raw_value = element.value.decode('utf-8')
        json_data = json.loads(raw_value)
        yield json_data
        
class EncodeMessage(beam.DoFn):
    
    def process(self, element):
        encoded_message = (element["id"].encode('utf-8'), json.dumps(element).encode('utf-8'))
        yield encoded_message

consumer_config_kafka_local = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "consumer-group-1",
    "auto.offset.reset": "latest",
    "enable.auto.commit": "true"
}


KAFKA_BOOTSTRAP_SERVERS="prod-broker.us-west1.gcp.confluent.cloud:9092"
CONSUMER_GROUP_ID="consumer-group-1"
KAFKA_USERNAME="username"
KAFKA_PASSWORD="password"
SECURITY_PROTOCOL="SASL_SSL"
SASL_MECHANISM="PLAIN"

consumer_config_kafka_cloud = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "group.id": CONSUMER_GROUP_ID,
    "sasl.jaas.config": f'org.apache.kafka.common.security.plain.PlainLoginModule required serviceName="Kafka" username="{KAFKA_USERNAME}" password="{KAFKA_PASSWORD}";',
    "security.protocol": SECURITY_PROTOCOL,
    "sasl.mechanism": SASL_MECHANISM,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": "false"
}

local_runner_options = PipelineOptions([
    "--runner=DirectRunner",
    "--direct_num_workers=1",
    "--direct_running_mode=in_memory"
])

local_runner_options.view_as(StandardOptions).streaming = True

mapping_config = {
            'leyland': {
                'id': 'supplier_id', 
                'name': 'supplier_name', 
                'vendor_list': {
                    '_source': 'supplier_data', 
                    '_items': {
                        'vendor': 'vendor_id', 
                        'name': 'vendor_name', 
                        'leyland_code': 'leyland_vendor_code', 
                        'DAF_code': 'DAF_vendor_code', 
                        'creation_date': 'creation_date', 
                        'last_update_date': 'last_update_date', 
                        'address': {
                            'street1': 'address.street1', 
                            'street2': 'address.street2', 
                            'street3': 'address.street3', 
                            'location': {
                                'postal_code': 'address.postal_code', 
                                'city': 'address.city', 
                                'country': 'address.country'
                            }
                        }
                    }
                }
            }
        }

def load_mapping_dict(config_path: str, mapping_name: str) -> dict:
        mapping_config = {}
        with open(config_path, "r") as file:
            config = yaml.safe_load(file)
            
            for item in config.get("topics", []):
                mapping_config[item["topic"]] = item["standardization_mapping"]

        if mapping_name not in config:
            raise KeyError(f"Mapping '{mapping_name}' not found in {config_path}")
        
        return mapping_config
    
mapping_config = load_mapping_dict(config_path="config/local.yaml", mapping_name="topics")
topics = list(mapping_config.keys())
    

with beam.Pipeline(options=local_runner_options) as p:
    ( 
        p
        | ReadFromKafka(
            consumer_config=consumer_config_kafka_local,
            topics= topics,
            max_num_records=1,
            with_metadata=True,
            expansion_service="localhost:8097"
        )
        | "DecodeMessage" >> beam.ParDo(DecodeMessage())
        | "StandardizeSchema" >> beam.ParDo(SchemaStandardizer(mapping_config=mapping_config))
        | "EncodeMessage" >> beam.ParDo(EncodeMessage()).with_output_types(tuple[bytes, bytes])
        | WriteToKafka(
            producer_config={"bootstrap.servers": "localhost:9092"},
            topic="output_topic",
            expansion_service="localhost:8097"
        )
    )