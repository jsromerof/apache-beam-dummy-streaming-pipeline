import apache_beam as beam
import argparse
from apache_beam.io.kafka import ReadFromKafka, WriteToKafka
from apache_beam.transforms.util import GroupIntoBatches
import logging
import yaml
from typing import List, Tuple
from collections import defaultdict
from pipelines.global_supplier_to_retio.src.schema_standardizer import SchemaStandardizer
from pipelines.global_supplier_to_retio.src.get_latest_version import GetLatestVersion
from pipelines.global_supplier_to_retio.utils.gcp_utils import get_project_id, get_secrets
from pipelines.global_supplier_to_retio.utils.pipeline_utils import configure_pipeline
from pipelines.global_supplier_to_retio.utils.kafka_utils import DecodeMessage, EncodeMessage


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
    
    '''
    PROJECT = get_project_id()
    Retrieving Kafka credentials from Secret Manager
    kafka_credentials = get_secrets(
        PROJECT, 
        config.get("kafka", {}).get("secret", {}).get("name"), 
        config.get("kafka", {}).get("secret", {}).get("version")
    )
    '''
    
    args, beam_args = get_args()
    config, pipeline_options = configure_pipeline(args=args, beam_args=beam_args)
    mapping_config = load_mapping_dict(config=config)
    
    kafka_credentials = {
        "user": '',
        "password": ''
    }
    
    consumer_config = config.get("kafka", {}).get("consumer", {}).get("config", {})
    if "sasl.jaas.config" in consumer_config:
        consumer_config["sasl.jaas.config"] = consumer_config.get("sasl.jaas.config", "").format(
            kafka_credentials["user"], kafka_credentials["password"]
        )
        
    producer_config = config.get("kafka", {}).get("producer", {}).get("config", {})
    if "sasl.jaas.config" in producer_config:
        producer_config["sasl.jaas.config"] = producer_config.get("sasl.jaas.config", "").format(
            kafka_credentials["user"], kafka_credentials["password"]
        )
        
    class DenormalizeMessage(beam.DoFn):
        
        def process(self, batch):
            fk_list_map = defaultdict(list)
            print("---------")
            print(batch)
            for element in batch:
                print("---------")
                print(element)
                source = element['source']
                fk_list_map[source].append(element['id'])
                
            print(fk_list_map)
                
            for element in batch:
                yield element
                
            
    class ProcessDiscardedMessages(beam.DoFn):
        """
        DoFn that process discarded messages for debugging and monitoring.
        """
        def process(self, batch):
            print(f"\n=== Processing Discarded Messages Batch with {len(batch)} elements ===")
            for i, element in enumerate(batch):
                print(f"Element {i+1}: {element}")
                logging.info(f"Processed discarded element {i+1}: {element}")
                yield element
            print("=== End of Batch ===\n")

    
    with beam.Pipeline(options=pipeline_options) as p:  
        streams_pcoll = ()
        
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
        
        versioned_messages = ( 
            streams_pcoll 
            | "PCollection Flatten" >> beam.Flatten()
            | "Add Key" >> beam.Map(lambda element: (element["supplier_id"], element))
            | "Group into Batches by Key" >> GroupIntoBatches(
                batch_size=3,
                max_buffering_duration_secs=10
            )
            | "Get Latest Message" >> beam.ParDo(GetLatestVersion()).with_outputs("latest_message", "older_messages")
        )
        
        (
            versioned_messages.older_messages
            | "Process Older Messages" >> beam.ParDo(ProcessDiscardedMessages())
        )
        
        (
            versioned_messages.latest_message
            | "StandardizeSchema" >> beam.ParDo(SchemaStandardizer(mapping_config=mapping_config))
            | "Group into Batches" >> beam.BatchElements(
                min_batch_size=1,
                max_batch_size=3,
                max_batch_duration_secs=2
            )
            | "Denormalize Message" >> beam.ParDo(DenormalizeMessage())
            | "EncodeMessage" >> beam.ParDo(EncodeMessage()).with_output_types(tuple[bytes, bytes])
            | WriteToKafka(
                producer_config=producer_config,
                topic=config.get("kafka", {}).get("producer", {}).get("topic"),
                expansion_service="localhost:8097"
            )
        )