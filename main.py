import argparse
import logging
import json

from toolkit.get_config import Config
from toolkit.instance_pool import DatabricksInstancePoolManager
 
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
    path_config = args.path_config  
  
    path = Config(path_config)
    config = path.get_config()

    if path.find_databricks() == 'databricks_instance_pool':
        insta = DatabricksInstancePoolManager(workspace_url=workspace_url, client_secret=client_secret, path_config=config )
        insta.create_or_edit_instance_pool(pool_name=, pool_config=path_config)

 


if __name__ == '__main__':
    main()