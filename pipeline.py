import apache_beam as beam
from apache_beam.io.kafka import ReadFromKafka, WriteToKafka
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import time
import json 
import time

options = PipelineOptions()
#options.view_as(StandardOptions).streaming = True


class DecodeMessage(beam.DoFn):
    
    def process(self, element):
        raw_value = element.value.decode('utf-8')
        json_data = json.loads(raw_value)
        yield json_data
  
        
class ProcessMessage(beam.DoFn):
    
    def process(self, element):
        element['processed_timestamp'] = time.time()
        element['status'] = 'processed'
        yield element
    
        
class EncodeMessage(beam.DoFn):
    
    def process(self, element):
        encoded_message = (element["chassis_id"].encode('utf-8'), json.dumps(element).encode('utf-8'))
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

STAGING_BUCKET="staging_bucket_poc" # Replace with your GCS bucket name
TEMP_BUCKET="temp_bucket_poc" 

dataflow_options = PipelineOptions([
    '--project=poc-project',
    '--region=us-west2',
    '--job_name=kafka-batch-processing-poc',
    '--staging_location=gs://{0}/'.format(STAGING_BUCKET),
    '--temp_location=gs://{0}/'.format(TEMP_BUCKET),
    '--network=gcp-dev-vpc',
    '--subnetwork=regions/us-west2/subnetworks/gcp-dev-subnet3',
    '--runner=DataflowRunner'
])

with beam.Pipeline(options=local_runner_options) as p:
    (
        p
        | ReadFromKafka(
            consumer_config=consumer_config_kafka_local,
            topics=["input_topic"],
            max_num_records=3,
            with_metadata=True,
            expansion_service="localhost:8097",
            #start_read_time=0000000000
        )
        | "DecodeMessage" >> beam.ParDo(DecodeMessage())
        | "ProcessMessage" >> beam.ParDo(ProcessMessage())
        | "EncodeMessage" >> beam.ParDo(EncodeMessage()).with_output_types(tuple[bytes, bytes])
        | WriteToKafka(
            producer_config={"bootstrap.servers": "localhost:9092"},
            topic="output_topic",
            expansion_service="localhost:8097"
        )
    )