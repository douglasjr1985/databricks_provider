import json
import logging

class Config:

    def __ini__(self, path_config):
        self.path_config = path_config

    def get_config(self,path_config):
        try:
            with open(f'{path_config}', 'r') as config_file:
                config = json.load(config_file)
            return config
        except FileNotFoundError:
            logging.error("File not found.")
        except (IOError, Exception) as e:
            logging.error(f"An unexpected error occurred: {e}")
        return None

    def find_databricks(self,path_config):   
        parts = path_config.split('/')

        # Procurando pelo segmento desejado
        for part in parts:
            if "databricks_instance_pool" in part:
                return part
        return None
