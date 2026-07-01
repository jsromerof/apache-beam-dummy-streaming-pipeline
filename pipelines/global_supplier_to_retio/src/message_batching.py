import apache_beam as beam

class MessageBatching(beam.DoFn):
    
    def __init__(self, batch_size: int):
        super().__init__()
        self.batch_size = batch_size
        self.current_batch = []

    def process(self, element):
        self.current_batch.append(element)
        if len(self.current_batch) >= self.batch_size:
            yield self.current_batch
            self.current_batch = []

    def finish_bundle(self):
        if self.current_batch:
            yield self.current_batch
            self.current_batch = []