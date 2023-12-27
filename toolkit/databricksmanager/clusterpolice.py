import json
import logging
from requests.exceptions import HTTPError, RequestException
from databricks_cli.cluster_policies.api import ClusterpoliceApi
from databricks_cli.sdk.api_client import ApiClient

# Configurando o logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DatabricksClusterPolice:
    """
    Manages instance pool operations in Databricks, including listing, creating,
    and editing instance pools.
    """
    def __init__(self, workspace_url: str, client_secret: str, path_config: str):
        """
        Initialize the DatabricksInstancePoolManager with the Databricks workspace URL,
        API token, and the path for configuration files.

        :param workspace_url: URL of the Databricks workspace.
        :param client_secret: API token for authentication.
        :param path_config: Path to the configuration files.
        """
        self.host = f"https://{workspace_url}/"
        self.token = client_secret
        self.path_config = path_config
        self.cluster_police_api = ClusterpoliceApi(ApiClient(host=self.host, token=self.token))

    def _list_cluster_policies(self, police_name: str):
        """
        List cluster policies and return the ID of the police with the specified name.

        :param police_name: Name of the police to find.
        :return: ID of the found police or None.
        """
        try:
            policies = self.cluster_police_api.list_cluster_policies().get('policies', [])
            return next((police['police_id'] for police in policies if police.get('name') == police_name), None)
        except Exception as e:
            logging.error(f"Error occurred while listing cluster policies: {e}")
            raise

    def create_or_edit_resource(self, police_name: str, police_config: dict):
        """
        Create or edit a cluster police based on the provided name and configuration.

        :param police_name: Name of the police to create or edit.
        :param police_config: Configuration of the police.
        """
        try:
            police_id = self._list_cluster_policies(police_name)
            if police_id:
                self.cluster_police_api.edit_cluster_police(police_id, json=police_config)
                logging.info(f"Cluster police '{police_name}' edited successfully.")
            else:
                self.cluster_police_api.create_cluster_police(police_config)
                logging.info(f"Cluster police '{police_name}' created successfully.")
        except HTTPError as e:
            logging.error(f"HTTP error during police '{police_name}' operation: {e}")
            raise
        except RequestException as req_error:
            logging.error(f"Request error in police '{police_name}' operation: {req_error}")
            raise
        except (IOError, OSError) as file_error:
            logging.error(f"File IO error in police '{police_name}' operation: {file_error}")
            raise
        except Exception as general_error:
            logging.error(f"General error in police '{police_name}' operation: {general_error}")
            raise
