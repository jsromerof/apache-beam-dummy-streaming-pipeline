import apache_beam as beam
import argparse
from apache_beam.io.kafka import ReadFromKafka, WriteToKafka
import json
import yaml
from typing import List, Tuple
from pipelines.global_supplier_to_retio.src.schema_standardizer import SchemaStandardizer
from pipelines.global_supplier_to_retio.utils.gcp_utils import get_project_id
from pipelines.global_supplier_to_retio.utils.pipeline_utils import configure_pipeline

class DecodeMessage(beam.DoFn):
    
    def process(self, element):
        raw_value = element.value.decode('utf-8')
        json_data = json.loads(raw_value)
        yield json_data
      
        
class EncodeMessage(beam.DoFn):
    
    def process(self, element):
        encoded_message = (element["id"].encode('utf-8'), json.dumps(element).encode('utf-8'))
        yield encoded_message


'''
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
'''


def get_args() -> Tuple[argparse.Namespace, List]:
        """Parses script-specific and beam-related arguments.

        Returns:
            Tuple[argparse.Namespace, List]: (script-specific args, beam args)
        """
        parser = argparse.ArgumentParser()

        parser.add_argument(
            "--params_file_path",
            required=True,
            type=str,
            help="Parameters of the pipeline.",
        )

        parser.add_argument(
            "--bucket_name",
            required=False,
            default=None,
            type=str,
            help="Cloud Storage bucket containing pipeline files.",
        )

        parser.add_argument(
            "--testing", action="store_true", help="Enable testing mode."
        )

        return parser.parse_known_args()
    
    
def get_config_file(file_path):
    with open(file_path, "r") as file:
        config = yaml.safe_load(file)
    return config


def load_mapping_dict(config: dict) -> dict:
    mapping_config = {}       
    for item in config.get("schema_standardization", []):
        mapping_config[item["topic"]] = config=get_config_file(item["standardization_mapping_file"])
    
    return mapping_config
    
    
if __name__ == "__main__":
    
    PROJECT = get_project_id()
    args, beam_args = get_args()
    config, pipeline_options = configure_pipeline(args=args, beam_args=beam_args)
    mapping_config = load_mapping_dict(config=config)
    
    with beam.Pipeline(options=pipeline_options) as p:
        streams_pcoll = ()
        consumer_config = config.get("kafka", {}).get("consumer", {}).get("config", {})
        for topic in config.get("kafka", {}).get("consumer", {}).get("topics", []):
            consumer_config.update({"group.id": topic["consumer_group"]})
            kafka_input_stream = (
                p
                | "Read from {0}".format(topic["topic"]) >> ReadFromKafka(
                    consumer_config=consumer_config,
                    topics=[topic["topic"]],
                    max_num_records=1,
                    with_metadata=True,
                    expansion_service="localhost:8097"
                )
                | "Decode Message from {0}".format(topic["topic"]) >> beam.ParDo(DecodeMessage())
            )
            streams_pcoll+=(kafka_input_stream,)
        ( 
            streams_pcoll 
            | "PCollection Flatten" >> beam.Flatten()
            | "StandardizeSchema" >> beam.ParDo(SchemaStandardizer(mapping_config=mapping_config))
            | "EncodeMessage" >> beam.ParDo(EncodeMessage()).with_output_types(tuple[bytes, bytes])
            | WriteToKafka(
                producer_config={"bootstrap.servers": "localhost:9092"},
                topic="output_topic",
                expansion_service="localhost:8097"
            )
        )