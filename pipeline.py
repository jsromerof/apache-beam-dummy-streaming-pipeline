import apache_beam as beam
from apache_beam.io.kafka import ReadFromKafka, WriteToKafka
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import time
import json 
import time

consumer_config = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "consumer-group-1",
    "auto.offset.reset": "earliest",
}

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


with beam.Pipeline() as p:
    (
        p
        | ReadFromKafka(
            consumer_config=consumer_config,
            topics=["input_topic"],
            max_num_records=1,
            with_metadata=True,
            expansion_service="localhost:8097",
            start_read_time=1779323662
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