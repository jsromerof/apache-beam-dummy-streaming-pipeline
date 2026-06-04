import apache_beam as beam
import argparse
from apache_beam.io.kafka import ReadFromKafka, WriteToKafka
import yaml
from typing import List, Tuple
from pipelines.global_supplier_to_retio.src.schema_standardizer import SchemaStandardizer
from pipelines.global_supplier_to_retio.utils.gcp_utils import get_project_id
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
    
    PROJECT = get_project_id()
    args, beam_args = get_args()
    config, pipeline_options = configure_pipeline(args=args, beam_args=beam_args)
    mapping_config = load_mapping_dict(config=config)
    
    with beam.Pipeline(options=pipeline_options) as p:
        
        streams_pcoll = ()
        consumer_config = config.get("kafka", {}).get("consumer", {}).get("config", {})
        producer_config = config.get("kafka", {}).get("producer", {}).get("config", {})
        
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
                producer_config=producer_config,
                topic=config.get("kafka", {}).get("producer", {}).get("topic"),
                expansion_service="localhost:8097"
            )
        )