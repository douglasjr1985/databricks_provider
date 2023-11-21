import logging
import json

from requests.exceptions import HTTPError, RequestException
from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.instance_pools.api import InstancePoolsApi

class DatabricksInstancePoolManager:
    """
    Manages instance pool operations in Databricks, including listing, creating,
    and editing instance pools.
    """
    def __init__(self, workspace_url, client_secret, path_config):
        """
        Initialize the DatabricksInstancePoolManager with the Databricks workspace URL,
        API token, and the path for configuration files.
        """
        self.host = f"https://{workspace_url}/"
        self.token = client_secret
        self.path_config = path_config
        self.instance_pools_api = InstancePoolsApi(ApiClient(host=self.host, token=self.token))

    def _list_instance_pools(self, pool_name: str):
        """
        List instance pools and return the ID of the pool with the specified name.
        Private method used internally.
        """
        try:
            pools_response = self.instance_pools_api.list_instance_pools()
            pools_list = pools_response.get('instance_pools', [])

            for pool in pools_list:
                if pool.get('instance_pool_name') == pool_name:
                    return pool.get('instance_pool_id')
        except Exception as e:
            logging.error(f"Error occurred: {e}")
        return None

    def create_or_edit_resource(self, pool_name: str, pool_config: dict):
        """
        Create or edit an instance pool based on the provided name and configuration.
        Public method to be called externally.
        """
        try:
            pool_id = self._list_instance_pools(pool_name)
            if pool_id:
                # Edit the existing pool if it's found
                self.instance_pools_api.edit_instance_pool(json=pool_config)
                logging.info(f"Instance pool '{pool_name}' edited successfully.")
            else:
                # Create a new pool if it doesn't exist
                self.instance_pools_api.create_instance_pool(pool_config)
                logging.info(f"Instance pool '{pool_name}' created successfully.")
        except HTTPError as e:
            logging.error(f"HTTP error during instance pool '{pool_name}': {e}")
        except RequestException as req_error:
            logging.error(f"HTTP request error in creating/editing instance pool '{pool_name}': {req_error}")
        except (IOError, OSError) as file_error:
            logging.error(f"File IO error in creating/editing instance pool '{pool_name}': {file_error}")
        except Exception as general_error:
            logging.error(f"General error in creating/editing instance pool '{pool_name}': {general_error}")