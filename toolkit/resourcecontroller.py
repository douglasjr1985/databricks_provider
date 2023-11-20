import json
import logging

from toolkit.databricksmanager.instance_pool import  DatabricksInstancePoolManager
from toolkit.databricksmanager.cluster import  DatabricksClusterManager


class Config:

    def __init__(self,workspace_url,client_secret, path_config):
       self.workspace_url = workspace_url
       self.client_secret = client_secret
       self.path_config = path_config

    def remove_json_extension(self, file_path: str):
        """
        Removes the .json extension from the file path.
        """
        if file_path.endswith('.json'):
            return file_path[:-5]  # Removes '.json'
        return None

    def load_config(self, file_path: str):
        """
        Loads the configuration from a JSON file.
        """
        try:
            with open(file_path, 'r') as config_file:
                return json.load(config_file)
        except FileNotFoundError:
            logging.error("File not found: " + file_path)
        except json.JSONDecodeError as e:
            logging.error("JSON decoding error: " + str(e))
        except Exception as e:
            logging.error("An unexpected error occurred: " + str(e))
        return None
    
    def execute(self):
        """
        Executes the configuration process and job execution.
        """
        file_path_without_extension = self.remove_json_extension(self.path_config)
        parts = file_path_without_extension.split('/')

        config = self.load_config(self.path_config)

        match parts:
            case [_, _, "databricks_instance_pool", pool_name, *_]:
                job_manager = DatabricksInstancePoolManager(self.workspace_url, self.client_secret, self.path_config)
                job_manager.create_or_edit_instance_pool(pool_name, config)
            case [_, _, "databricks_cluster", cluster_name, *_]:
                cluster_manager = DatabricksClusterManager(self.workspace_url, self.client_secret, self.path_config)
                cluster_manager.create_or_edit_cluster(cluster_name, config)