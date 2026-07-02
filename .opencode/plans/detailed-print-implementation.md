# Detailed Implementation Plan: Add Batch Element Printing to Apache Beam Pipeline

## Target File: `pipelines/global_supplier_to_retio/pipeline.py`

## Current Pipeline Structure
The pipeline currently processes Kafka messages in these stages:
1. Reads from Kafka topics
2. Decodes messages
3. Flattens multiple topic streams
4. Groups by supplier_id into batches
5. Gets latest message version
6. Creates final batches of 3 elements

## Implementation Details

### Step 1: Add Printing DoFn Class
**Location**: After line 126 (after the `versioned_messages` PCollection definition)

**Code to insert**:
```python
        # Print each element of the batch for visibility
        class PrintElements(beam.DoFn):
            """
            DoFn that prints each element in a batch for debugging and monitoring.
            """
            def process(self, batch):
                print(f"\n=== Processing Batch with {len(batch)} elements ===")
                for i, element in enumerate(batch):
                    print(f"Element {i+1}: {element}")
                    logging.info(f"Processed batch element {i+1}: {element}")
                    yield element
                print("=== End of Batch ===\n")
```

### Step 2: Add Pipeline Transform
**Location**: Immediately after the PrintElements class (line 126+)

**Code to insert**:
```python
        # Add printing step to visualize batch elements
        (
            versioned_messages
            | "Print Batch Elements" >> beam.ParDo(PrintElements())
        )
```

## Line Number Changes
The implementation will add approximately 25 lines to the file, shifting subsequent line numbers.

## Benefits
1. **Visibility**: See exactly what data is being processed in each batch
2. **Debugging**: Easy to identify data issues or transformations
3. **Monitoring**: Track batch processing in real-time
4. **Non-intrusive**: Doesn't modify data, just prints and yields

## Integration Notes
- The transform maintains the pipeline flow by yielding each element
- Uses standard Python print() and logging for output
- No impact on pipeline performance or data integrity
- Compatible with existing transforms

## Testing Expected Output
When the pipeline runs, you'll see output like:
```
=== Processing Batch with 3 elements ===
Element 1: {'supplier_id': '123', 'data': {...}}
Element 2: {'supplier_id': '124', 'data': {...}}
Element 3: {'supplier_id': '125', 'data': {...}}
=== End of Batch ===
```