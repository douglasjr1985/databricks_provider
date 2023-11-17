import json
import logging

from toolkit.instance_pool import  DatabricksInstancePoolManager

class Config:

    def __init__(self,workspace_url,client_secret, path_config):
       self.workspace_url = workspace_url
       self.client_secret = client_secret
       self.path_config = path_config

    def remove_json_extension(self, file_path):
        """
        Removes the .json extension from the file path.
        """
        if file_path.endswith('.json'):
            return file_path[:-5]  # Removes '.json'
        return None

    def load_config(self, file_path):
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
        if file_path_without_extension:
            parts = file_path_without_extension.split('/')
            config = self.load_config(self.path_config)
            if parts and len(parts) > 3 and parts[2] == 'databricks_instance_pool':
                job_manager = DatabricksInstancePoolManager(self.workspace_url, self.client_secret, self.path_config)
                job_manager.create_or_edit_instance_pool(parts[3], config)
 