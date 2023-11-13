import argparse
import logging
import json

from toolkit.get_config import _get_job_config
from computer.databricks_instance_pool.instance_pool import DatabricksInstancePoolManager 

computer/databricks_cluster/resource/databricks_cluster.json

def _get_job_config(path_config):
    try:
        with open(f'{path_config}', 'r') as config_file:
            job_config = json.load(config_file)
        return job_config
    except FileNotFoundError:
        logging.error("File not found.")
    except (IOError, Exception) as e:
        logging.error(f"An unexpected error occurred: {e}")
    return None

def find_databricks_instance_pool(text):
    parts = text.split('/')

    # Procurando pelo segmento desejado
    for part in parts:
        if "databricks_instance_pool" in part:
            return part
    return None


def main():
    # Logging configuration
    logging.basicConfig(filename='log_file.log', level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Argument configuration
    parser = argparse.ArgumentParser(description='Process modified Jobs.')
    parser.add_argument('--workspace_url', type=str, help='Workspace URL')
    parser.add_argument('--client_secret', type=str, help='Client Secret')
    parser.add_argument('--path_config', type=str, help='Path Config')
    args = parser.parse_args()

    logging.info(f'Processing job: {args.path_config}') 

    workspace_url = args.workspace_url
    client_secret = args.client_secret
    path_config = args.filename  


    #path_config = _get_job_config(path_config)

    if path_config:
        job_manager = DatabricksInstancePoolManager(workspace_url, client_secret)
        job_manager.create_or_replace_job(job_name, path_config)
    else:
        logging.error('Unable to create or update the job due to previous errors.')


if __name__ == '__main__':
    main()