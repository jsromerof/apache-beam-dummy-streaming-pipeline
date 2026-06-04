import apache_beam as beam
import json

class DecodeMessage(beam.DoFn):
    def process(self, element):
        raw_value = element.value.decode('utf-8')
        json_data = json.loads(raw_value)
        yield json_data
        
class EncodeMessage(beam.DoFn):
    def process(self, element):
        encoded_message = (element["id"].encode('utf-8'), json.dumps(element).encode('utf-8'))
        yield encoded_message