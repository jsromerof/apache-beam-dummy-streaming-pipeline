import apache_beam as beam
from apache_beam.io.kafka import ReadFromKafka, WriteToKafka
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import json 
import psycopg
from psycopg.rows import dict_row

class DecodeMessage(beam.DoFn):
    
    def process(self, element):
        raw_value = element.value.decode('utf-8')
        json_data = json.loads(raw_value)
        yield json_data
  
        
class ProcessMessage(beam.DoFn):
    
    def __init__(self, uri=None):
        super().__init__()
        self.uri = uri

    def process(self, element):
        with psycopg.connect(self.uri, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(f"""INSERT INTO SUPPLIER(SUPPLIER_ID, SUPPLIER_NUMBER, SUPPLIER_NAME, IS_LOCKED, LOAD_DATE_TIME, UPDATE_DATE_TIME)
                    VALUES (
                        {element['supplier_id']}, 
                        '{element['supplier_number']}', 
                        '{element['supplier_name']}', 
                        {element['is_locked']}, 
                        '{element['load_date_time']}', 
                        '{element['update_date_time']}')
                    ON CONFLICT(SUPPLIER_ID)
                    DO UPDATE SET
                        SUPPLIER_NUMBER=EXCLUDED.SUPPLIER_NUMBER,
                        SUPPLIER_NAME=EXCLUDED.SUPPLIER_NAME,
                        IS_LOCKED=EXCLUDED.IS_LOCKED,
                        UPDATE_DATE_TIME=EXCLUDED.UPDATE_DATE_TIME
                    WHERE 
                        SUPPLIER.UPDATE_DATE_TIME < EXCLUDED.UPDATE_DATE_TIME
                    RETURNING *;"""
                )
                result = cur.fetchone()
                if result:
                    print("Database updated for supplier_id: {0}".format(element['supplier_id']))
                else:
                    print("No updates were made to the database for supplier_id: {0}".format(element['supplier_id']))
        
        yield element
                
                
class EncodeMessage(beam.DoFn):
    
    def process(self, element):
        encoded_message = (element["supplier_id"].encode('utf-8'), json.dumps(element).encode('utf-8'))
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

dataflow_options.view_as(StandardOptions).streaming = True

kafka_input_topics = ["input_topic"]

db_uri = "postgresql://postgres:postgres@localhost:5432/postgres"

with beam.Pipeline(options=local_runner_options) as p:
    kafka_streams_pcoll = ()
    for topic in kafka_input_topics:
        input_stream = (
            p
            | "Read from {0}".format(topic) >> ReadFromKafka(
                consumer_config=consumer_config_kafka_local,
                topics=[topic],
                max_num_records=1,
                with_metadata=True,
                expansion_service="localhost:8097"
            )
            | "Decode Message from {0}".format(topic) >> beam.ParDo(DecodeMessage())
        )
        kafka_streams_pcoll+=(input_stream,)
        
    (
        kafka_streams_pcoll 
        | "PCollection Flatten" >> beam.Flatten()
        | "ProcessMessage" >> beam.ParDo(ProcessMessage(uri=db_uri))
        | "EncodeMessage" >> beam.ParDo(EncodeMessage()).with_output_types(tuple[bytes, bytes])
        | WriteToKafka(
            producer_config={"bootstrap.servers": "localhost:9092"},
            topic="output_topic",
            expansion_service="localhost:8097"
        )
    )