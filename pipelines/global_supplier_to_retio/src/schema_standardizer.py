from glom import glom
import apache_beam as beam
from glom import glom, Spec, T
import json
import yaml

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


# --- Example usage ---
if __name__ == "__main__":
    source = {
        "topic" : "leyland",
        "supplier_id":   1,
        "supplier_name": "supplier_1",
        "supplier_data": [
            {
                "vendor_id":           424216,
                "vendor_name":         "JOPP INTERIOR GMBH",
                "leyland_vendor_code": "H16457",
                "DAF_vendor_code":     "99123",
                "creation_date":       "2013-04-08T11:28:43",
                "last_update_date":    "2019-09-10T13:23:14",
                "address": {
                    "street1":     "Am Jopp 1",
                    "street2":     "Additional Street Info",
                    "street3":     "More Street Info",
                    "postal_code": "58566",
                    "city":        "Kierspe",
                    "country":     "Germany"
                }
            },
            {
                "vendor_id":           528242,
                "vendor_name":         "OHM HANER METALLWERK GMBH CO KG",
                "leyland_vendor_code": "003554",
                "DAF_vendor_code":     "87148",
                "creation_date":       "2014-10-24T13:01:59",
                "last_update_date":    "2019-09-19T15:41:47",
                "address": {
                    "street1":     "Am Jopp 1",
                    "street2":     "Additional Street Info",
                    "street3":     "More Street Info",
                    "postal_code": "58566",
                    "city":        "Kierspe",
                    "country":     "Germany"
                }
            },
            {
                "vendor_id":           528243,
                "vendor_name":         "PETER GFK SPOL SRO",
                "leyland_vendor_code": "P17032",
                "DAF_vendor_code":     "86810",
                "creation_date":       "2014-10-24T13:08:06",
                "last_update_date":    "2019-09-19T15:41:47",
                "address": {
                    "street1":     "ul. PETER 1",
                    "street2":     "Additional Street Info",
                    "street3":     "More Street Info",
                    "postal_code": "58566",
                    "city":        "Kierspe",
                    "country":     "Germany"
                }
            }
        ]
    }
    
    
    def load_mapping_dict(config_path: str, mapping_name: str) -> dict:
        mapping_config = {}
        with open(config_path, "r") as file:
            config = yaml.safe_load(file)
            
            for item in config.get("topics", []):
                mapping_config[item["topic"]] = item["standardization_mapping"]

        if mapping_name not in config:
            raise KeyError(f"Mapping '{mapping_name}' not found in {config_path}")
        
        return mapping_config
    
    mapping_config = load_mapping_dict(config_path="local.yaml", mapping_name="topics")
    
    print(mapping_config)
    result = standardize_messsage(source, mapping_config)
    print(json.dumps(result, indent=4))