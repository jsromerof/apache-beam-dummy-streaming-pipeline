import apache_beam as beam
from apache_beam.io.kafka import ReadFromKafka, WriteToKafka
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import json
import yaml
from pipelines.global_supplier_to_retio.src.schema_standardizer import SchemaStandardizer

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


KAFKA_BOOTSTRAP_SERVERS="dev-broker.us-west1.gcp.confluent.cloud:9092"
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


def get_config_file(file_path):
    with open(file_path, "r") as file:
        config = yaml.safe_load(file)
    return config


def load_mapping_dict(config: dict) -> dict:
        mapping_config = {}       
        for item in config.get("topics", []):
            mapping_config[item["topic"]] = config=get_config_file(item["standardization_mapping_file"])
        
        return mapping_config
    
config=get_config_file("pipelines/global_supplier_to_retio/parametrization/LOCAL/config.yaml")
mapping_config = load_mapping_dict(config=config)
topics = list(mapping_config.keys())
    

with beam.Pipeline(options=local_runner_options) as p:
    kafka_streams_pcoll = ()
    kafka_consumer_config = config["kafka_consumer_config"]
    for topic in config["topics"]:
        kafka_consumer_config.update({"group.id": topic["consumer_group"]})
        kafka_input_stream = (
            p
            | "Read from {0}".format(topic["topic"]) >> ReadFromKafka(
                consumer_config=consumer_config_kafka_local,
                topics=[topic["topic"]],
                max_num_records=1,
                with_metadata=True,
                expansion_service="localhost:8097"
            )
            | "Decode Message from {0}".format(topic["topic"]) >> beam.ParDo(DecodeMessage())
        )
        kafka_streams_pcoll+=(kafka_input_stream,)
    ( 
        kafka_streams_pcoll 
        | "PCollection Flatten" >> beam.Flatten()
        | "StandardizeSchema" >> beam.ParDo(SchemaStandardizer(mapping_config=mapping_config))
        | "EncodeMessage" >> beam.ParDo(EncodeMessage()).with_output_types(tuple[bytes, bytes])
        | WriteToKafka(
            producer_config={"bootstrap.servers": "localhost:9092"},
            topic="output_topic",
            expansion_service="localhost:8097"
        )
    )