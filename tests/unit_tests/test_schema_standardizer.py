import pytest
import yaml
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, is_not_empty, equal_to
from src.schema_standardizer import SchemaStandardizer

# ------------------------------------------------------------------ #
#  Fixtures                                                            #
# ------------------------------------------------------------------ #

@pytest.fixture
def config_file(tmp_path):
    """Write a temp YAML config and return its path."""
    config = {
        "topics": [
            {
                "topic": "leyland",
                "standardization_mapping": {
                    "id":   "supplier_id",
                    "name": "supplier_name",
                    "vendor_list": {
                        "_source": "supplier_data",
                        "_items": {
                            "vendor":           "vendor_id",
                            "name":             "vendor_name",
                            "leyland_code":     "leyland_vendor_code",
                            "DAF_code":         "DAF_vendor_code",
                            "creation_date":    "creation_date",
                            "last_update_date": "last_update_date",
                            "address": {
                                "street1":     "address.street1",
                                "street2":     "address.street2",
                                "postal_code": "address.postal_code",
                                "city":        "address.city",
                                "country":     "address.country",
                            }
                        }
                    }
                }   
            }
        ]
    }
    path = tmp_path / "mapping_config.yaml"
    path.write_text(yaml.dump(config))
    return str(path)


@pytest.fixture
def supplier_source():
    return {
        "supplier_id":   "1",
        "supplier_name": "supplier_1",
        "supplier_data": [
            {
                "vendor_id":           "424216",
                "vendor_name":         "JOPP INTERIOR GMBH",
                "leyland_vendor_code": "H16457",
                "DAF_vendor_code":     "99123",
                "creation_date":       "2013-04-08T11:28:43",
                "last_update_date":    "2019-09-10T13:23:14",
                "address": {
                    "street1":     "Am Jopp 1",
                    "street2":     "Additional Street Info",
                    "postal_code": "58566",
                    "city":        "Kierspe",
                    "country":     "Germany"
                }
            },
            {
                "vendor_id":           "528242",
                "vendor_name":         "OHM HANER METALLWERK GMBH CO KG",
                "leyland_vendor_code": "003554",
                "DAF_vendor_code":     "87148",
                "creation_date":       "2014-10-24T13:01:59",
                "last_update_date":    "2019-09-19T15:41:47",
                "address": {
                    "street1":     "Am Jopp 2",
                    "street2":     "Second Street Info",
                    "postal_code": "58567",
                    "city":        "Berlin",
                    "country":     "Germany"
                }
            }
        ]
    }


# ------------------------------------------------------------------ #
#  Transform tests                                                    #
# ------------------------------------------------------------------ #

class TestTransform:

    def test_extract_leyland_supplier_data(self):
        # Test data
        test_message = {
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
        
        mapping_config = {
            'leyland': {
                'id': 'supplier_id', 
                'name': 'supplier_name', 
                'vendor_list': {
                    '_source': 'supplier_data', 
                    '_items': {
                        'vendor': 'vendor_id', 
                        'name': 'vendor_name', 
                        'leyland_code': 'leyland_vendor_code', 
                        'DAF_code': 'DAF_vendor_code', 
                        'creation_date': 'creation_date', 
                        'last_update_date': 'last_update_date', 
                        'address': {
                            'street1': 'address.street1', 
                            'street2': 'address.street2', 
                            'street3': 'address.street3', 
                            'location': {
                                'postal_code': 'address.postal_code', 
                                'city': 'address.city', 
                                'country': 'address.country'
                            }
                        }
                    }
                }
            }
        }
        
        # Create test pipeline
        with TestPipeline() as pipeline:
            result = (
                pipeline
                | beam.Create([test_message])
                | beam.ParDo(SchemaStandardizer(mapping_config=mapping_config))
            )
            
            # Verify the results
            assert_that(result, equal_to([
                {
                    "id": 1,
                    "name": "supplier_1",
                    "vendor_list": [
                        {
                            "vendor": 424216,
                            "name": "JOPP INTERIOR GMBH",
                            "leyland_code": "H16457",
                            "DAF_code": "99123",
                            "creation_date": "2013-04-08T11:28:43",
                            "last_update_date": "2019-09-10T13:23:14",
                            "address": {
                                "street1": "Am Jopp 1",
                                "street2": "Additional Street Info",
                                "street3": "More Street Info",
                                "location": {
                                    "postal_code": "58566",
                                    "city": "Kierspe",
                                    "country": "Germany"
                                }
                            }
                        },
                        {
                            "vendor": 528242,
                            "name": "OHM HANER METALLWERK GMBH CO KG",
                            "leyland_code": "003554",
                            "DAF_code": "87148",
                            "creation_date": "2014-10-24T13:01:59",
                            "last_update_date": "2019-09-19T15:41:47",
                            "address": {
                                "street1": "Am Jopp 1",
                                "street2": "Additional Street Info",
                                "street3": "More Street Info",
                                "location": {
                                    "postal_code": "58566",
                                    "city": "Kierspe",
                                    "country": "Germany"
                                }
                            }
                        },
                        {
                            "vendor": 528243,
                            "name": "PETER GFK SPOL SRO",
                            "leyland_code": "P17032",
                            "DAF_code": "86810",
                            "creation_date": "2014-10-24T13:08:06",
                            "last_update_date": "2019-09-19T15:41:47",
                            "address": {
                                "street1": "ul. PETER 1",
                                "street2": "Additional Street Info",
                                "street3": "More Street Info",
                                "location": {
                                    "postal_code": "58566",
                                    "city": "Kierspe",
                                    "country": "Germany"
                                }
                            }
                        }
                    ]
                }
            ]))