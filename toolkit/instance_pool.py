import logging
from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.instance_pools.api import InstancePoolsApi

class DatabricksInstancePoolManager:
    """
    Manages instance pool operations in Databricks, including listing, creating,
    and editing instance pools.
    """

    def __init__(self, workspace_url, client_secret):
        """
        Initializes the manager with the workspace URL and API token for Databricks.
        """
        self.host = f"https://{workspace_url}/"
        self.token = client_secret
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
        except Exception as e:
            logging.error(f"Error occurred: {e}")
        return None

    def create_or_edit_instance_pool(self, pool_name, pool_config):
        """
        Creates or edits an instance pool based on the provided name and configuration.
        """
        try:    
            pool_id = self._list_instance_pools(pool_name)
            if pool_id is not None:
                self.instance_pools_api.edit_instance_pool(json=pool_config)
                logging.info(f"Instance pool '{pool_name}' edited successfully.")
            else:
                self.instance_pools_api.create_instance_pool(pool_config)
                logging.info(f"Instance pool '{pool_name}' created successfully.")                

        except Exception as e:
            logging.error(f"Error occurred while creating or editing instance pool '{pool_name}': {e}")                