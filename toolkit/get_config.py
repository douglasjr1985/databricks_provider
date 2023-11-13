import json
import logging
import argparse


def _get_job_config(job_name):
    try:
        with open(f'resources/{job_name}.json', 'r') as config_file:
            job_config = json.load(config_file)
        return job_config
    except FileNotFoundError:
        logging.error("File not found.")
    except (IOError, Exception) as e:
        logging.error(f"An unexpected error occurred: {e}")
    return None

