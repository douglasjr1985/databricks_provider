import logging
from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.instance_pools.api import InstancePoolsApi
from databricks_cli.sdk.exceptions import HTTPError

class DatabricksInstancePoolManager:
    """
    Manages instance pool operations in Databricks, including listing, creating,
    and editing instance pools.
    """

    def __init__(self, workspace_url, api_token):
        """
        Initializes the manager with the workspace URL and API token for Databricks.
        """
        self.host = f"https://{workspace_url}/"
        self.token = api_token
        self.instance_pools_api = InstancePoolsApi(
            ApiClient(host=self.host, token=self.token)
        )

    def _list_instance_pools(self, pool_name):
        """
        Private method to list instance pools and return the ID of the pool
        with the specified name.
        """
        try: 
            pools = self.instance_pools_api.list_instance_pools()
            for _, items in pools.items():
                for pool in items:
                    if pool['instance_pool_name'] == pool_name:
                        return pool['instance_pool_id']
        except HTTPError as e:
            logging.error(f"HTTP error occurred: {e}")
        except Exception as e:
            logging.error(f"Unexpected error occurred: {e}")
        return None

    def create_instance_pool(self, pool_config):
        """
        Creates a new instance pool with the given configuration.
        """
        try:
            self.instance_pools_api.create_instance_pool(pool_config)
            logging.info("Instance pool created successfully.")
        except HTTPError as e:
            logging.error(f"HTTP error during pool creation: {e}")
            return False
        return True

    def edit_instance_pool(self, pool_id, pool_config):
        """
        Edits an existing instance pool based on the provided pool ID and configuration.
        """
        try:
            self.instance_pools_api.edit_instance_pool(pool_id, pool_config)
            logging.info("Instance pool edited successfully.")
        except HTTPError as e:
            logging.error(f"HTTP error during pool editing: {e}")
            return False
        return True

    def create_or_edit_instance_pool(self, pool_name, pool_config):
        """
        Creates or edits an instance pool based on the provided name and configuration.
        """
        pool_id = self._list_instance_pools(pool_name)
        if pool_id:
            return self.edit_instance_pool(pool_id, pool_config)
        else:
            return self.create_instance_pool(pool_config)
