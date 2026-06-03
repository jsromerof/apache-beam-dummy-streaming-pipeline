from pipelines.global_supplier_to_retio.utils.read_configs import download_file_from_bucket
import logging
import yaml
import argparse
from typing import Dict, List, Tuple, Optional
from apache_beam.options.pipeline_options import PipelineOptions

class PipelineConfigError(Exception):
    """Handles errors associated to pipeline configuration"""

    def __init__(self, msg=""):
        self.msg = msg
        logging.error(msg)

    def __str__(self):
        return self.msg


def beam_args_to_dict(beam_args: List) -> Dict:
    """Maps beam arguments into a dictionary

    Args:
        beam_args (List): Beam arguments

    Returns:
        Dict: contains beam arguments as key: arg_name, value: arg value
    """

    beam_args_dict = {}
    for x in beam_args:
        try:
            k_raw, v = x.split("=")
            k = k_raw.replace("--", "")
            beam_args_dict[k] = v
        except ValueError:
            logging.warning(f"Parsing beam argumenst failed for arg: {x}")
    return beam_args_dict


def configure_pipeline(
    args: argparse.Namespace, beam_args: List
) -> Tuple[Dict, PipelineOptions]:
    """Setup the pipeline configuration using:
    1. Pipeline parameters read from a configuration file
    2. beam specific arguments provided via command line
    These two sources are used to build:
    1. pipeline_parameters: containig parameters for PTransforms
    2. pipeline_options: containing parametes for the beam Pipeline object.

    Args:
        args (argparse.Namespace): script-specific arguments
        beam_args (List): beam-related arguments

    Raises:
        PipelineConfigError: handles pipeline parameters parsing errors

    Returns:
        Tuple[Dict, PipelineOptions]: (pipeline_parameters, pipeline_options)

    Below is an example of how to use it in your pipeline.py. Take into account that 
    get_args will simple parse script-specific and beam-related arguments like 
    params_file_path and bucket_name:

    args, beam_args = get_args()
    pipeline_parameters, pipelines_options = configure_pipeline(
        args=args, beam_args=beam_args
    )
    """

    beam_options = beam_args_to_dict(beam_args=beam_args)

    runner = (
        beam_options["runner"] if "runner" in beam_options.keys() else "DataflowRunner"
    )
    pipeline_options = PipelineOptions(
        beam_args,
        save_main_session=True,
        runner=runner,
        use_public_ips=False,
        setup_file="./setup.py",
    )

    pipeline_params_filepath = args.params_file_path
    if args.bucket_name:
        download_filename = "config.yaml"
        download_file_from_bucket(
            args.bucket_name, args.params_file_path, download_filename
        )
        pipeline_params_filepath = download_filename

    with open(pipeline_params_filepath, "r") as f:
        try:
            pipeline_parameters = yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise PipelineConfigError(str(e))

    return pipeline_parameters, pipeline_options