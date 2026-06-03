import yaml
import os
from urllib.parse import urlparse
import datetime
from typing import Literal, List
from google.cloud import storage
import google
import tempfile


def download_file_from_bucket(bucket_name: str, key: str, destination_file_name: str):
    """Downloads a file from a Google Cloud Storage bucket.
    Args:
        bucket_name (str): GCS bucket name storing the parameters file
        key (str): parameter file path
        destination_file_name (str): downloaded file name
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(key)
    try:
        blob.download_to_filename(destination_file_name)
    except google.cloud.exceptions.NotFound as ex:
        raise Exception(f"blob from GCS {bucket_name}/{key} does not exist")

def _fix_configs(
        configs:dict, 
        int_fields:list = ['max_num_records'],
        datetime_fields:list = ['start_read_time'])-> dict:
    """
    Alter the config dictionary depending on parameters such as int_fields and datetime_fields

    This is more of an example. This could be greatly expanded to include different types of dates, times, 
    floats, etc.

    Arguments:
       configs: dict, what is read from yaml
       int_fields: list, a list of fields you wish to convert to integers
       datetime_fields: list, a list of fields you wish to convert to datetime

    Returns:
       dict of configs
    """
    d = {}
    for key in configs.keys():
        if key in int_fields:
            if configs[key] == 'None' or configs[key] == 'none':
                d[key] = None
            else:
                d[key] = int(configs[key])
        elif key in datetime_fields:
            if configs[key] == 'None' or configs[key] == 'none':
                d[key] = None
            else:
                try:
                    d[key] = datetime.datetime.strptime(configs[key], '%Y-%m-%d %H:%M:%S')
                except ValueError:
                    d[key] = configs[key]
        else:
            d[key] = configs[key]
    return d

def get_config_path(base_path:str) -> str:
    """
    get the directory where the config file is

    Arguments:
      base_path: str, the path of the main python file (pipeline.py)

    Returns:
       str, the path to config.yaml

    """
    base_dir = os.path.dirname(os.path.abspath(base_path))
    return  os.path.join(base_dir, 'parametrization', 'config.yaml')

def _get_bucket_name_key(path:str) -> (str, str):
    """
    if gs:// is in path, get bucket_name and key
    """
    url_parts = urlparse(path, allow_fragments=False)
    bucket_name = url_parts.netloc
    key = url_parts.path
    if key[0]=="/":
        key=key[1:]
    return bucket_name, key

def _make_local_from_blob(bucket_name:str, key:str)-> (object, str):
    """
    Download blob and write to a tempfile. Return fh and path
    """
    fh, path = tempfile.mkstemp()
    download_file_from_bucket(
            bucket_name = bucket_name, 
            key = key, 
            destination_file_name = path
            )
    return  fh, path

def get_configs(
        path:str ,
        int_fields: List[str] = [],
        datetime_fields: List[str] = [],
        bucket_name:str = None,
        key:str = None,
        )-> dict:
    """
    return configs from yaml path

    Arguments:
       path: str, path to yaml file 
       int_fields: list of int_fields needed to be converted to integers
       datetime_fields: list of datetime fields  needed to be converted to integers

    Returns: 
       dict
    """
    local_path = path
    if path.startswith('gs:'):
        bucket_name, key = _get_bucket_name_key(path)
    if bucket_name:
        fh, local_path = _make_local_from_blob(bucket_name = bucket_name, key = key)
    if not os.path.isfile(local_path):
        raise Exception(f'file "{path}" not found')
    with open(local_path, 'r') as read_obj:
        try:
            d =  yaml.safe_load(read_obj)
        except yaml.YAMLError as exc:
            raise(Exception(str(exc)))
        finally:
            if bucket_name:
                os.close(fh)
                os.remove(local_path)
    return _fix_configs(
            configs = d,
            int_fields = int_fields,
            datetime_fields = datetime_fields
            )


def get_mult_configs(
        paths:List[str],
        int_fields: List[str] = [],
        datetime_fields: List[str] = [],
        )-> dict:
    """
    Get a dictionary of configs from 1 or more yaml file
    Arguments:
       paths: list, list of yaml files

    Returns:
       dict of configs

    """
    assert len(paths) > 0
    main_dict = {}
    for path in paths:
        d = get_configs(
                path = path,
                int_fields = int_fields,
                datetime_fields = datetime_fields,
                )
        main_dict = main_dict | d
    return main_dict