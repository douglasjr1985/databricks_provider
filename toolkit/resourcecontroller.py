import json
import logging

from toolkit.databricksmanager.instancepool import  DatabricksInstancePoolManager
from toolkit.databricksmanager.cluster import  DatabricksClusterManager
from toolkit.databricksmanager.clusterpolice import  DatabricksClusterPolice

class Config:

    def __init__(self,workspace_url,client_secret, path_config):
       self.workspace_url = workspace_url
       self.client_secret = client_secret
       self.path_config = path_config

    def _remove_json_extension(self, file_path: str):
        """
        Removes the .json extension from the file path.
        """
        if file_path.endswith('.json'):
            return file_path[:-5]  # Removes '.json'
        return None

    def _load_config(self, file_path: str):
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
        file_path_without_extension = self._remove_json_extension(self.path_config)
        parts = file_path_without_extension.split('/')

        config = self._load_config(self.path_config)

        match parts:
            case [_, _, "databricks_instance_pool", pool_name, *_]:
                self.manage_databricks_resource(DatabricksInstancePoolManager, pool_name, config)
            case [_, _, "databricks_cluster", cluster_name, *_]:
                self.manage_databricks_resource(DatabricksClusterManager, cluster_name, config)
            case [_, _, "databricks_cluster_policy", policy_name, *_]:
                self.manage_databricks_resource(DatabricksClusterPolice, policy_name, config)                

    def manage_databricks_resource(self, manager_class, resource_name, config):
        """
        Manages a Databricks resource.
        """
        manager = manager_class(self.workspace_url, self.client_secret, self.path_config)
        manager.create_or_edit_resource(resource_name, config)