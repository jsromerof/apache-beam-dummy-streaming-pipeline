from glom import glom
import apache_beam as beam
from glom import glom

# Spec for each vendor item — flattens the nested address into the parent level
# Reserved keys used as markers in the config
_LIST_SOURCE_KEY = "_source"
_LIST_ITEMS_KEY  = "_items"


def build_spec(node: dict | str) -> dict | str | tuple:
    """
    Recursively builds a glom spec from a config node.

    Rules:
      - str                          → simple dot-path  e.g. "address.street1"
      - dict with _source + _items  → list spec         e.g. ("supplier_data", [item_spec])
      - dict (anything else)         → nested object     e.g. {"street1": "address.street1", ...}
    """
    # Base case: plain string → dot-path
    if isinstance(node, str):
        return node

    if not isinstance(node, dict):
        raise ValueError(f"Unsupported config node type: {type(node)} → {node}")

    # List mapping
    if _LIST_SOURCE_KEY in node and _LIST_ITEMS_KEY in node:
        source_path = node[_LIST_SOURCE_KEY]
        item_spec   = build_spec(node[_LIST_ITEMS_KEY])
        return (source_path, [item_spec])

    # Nested object or top-level spec — recurse on every key
    return {
        target_key: build_spec(value)
        for target_key, value in node.items()
        if not target_key.startswith("_")   # skip any reserved markers
    }


def standardize_messsage(element: dict, mapping_config: dict) -> dict:
    spec = build_spec(mapping_config[element["topic"]])
    return glom(element, spec)

class SchemaStandardizer(beam.DoFn):
    
    def __init__(self, mapping_config: dict):
        super().__init__()
        self.mapping_config  = mapping_config
    
    def process(self, element):
        standardized_message = standardize_messsage(element, mapping_config=self.mapping_config)
        yield standardized_message