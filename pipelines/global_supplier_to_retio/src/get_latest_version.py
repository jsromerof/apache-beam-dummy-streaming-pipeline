import apache_beam as beam

class GetLatestVersion(beam.DoFn):
    def process(self, element):
        key, messages = element
        latest_message = max(messages, key=lambda x: x.get("timestamp", 0))
        older_messages = [msg for msg in messages if msg != latest_message]
        for msg in older_messages:
            yield beam.pvalue.TaggedOutput("older_messages", msg)
        yield beam.pvalue.TaggedOutput("latest_message", (key, latest_message))