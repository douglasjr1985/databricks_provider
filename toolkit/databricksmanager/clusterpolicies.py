import json
import logging
from requests.exceptions import HTTPError, RequestException
from databricks_cli.cluster_policies.api import ClusterPolicyApi
from databricks_cli.sdk.api_client import ApiClient

# Configurando o logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DatabricksInstancePoolManager:
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
        self.cluster_policy_api = ClusterPolicyApi(ApiClient(host=self.host, token=self.token))

    def _list_cluster_policies(self, policy_name: str):
        """
        List cluster policies and return the ID of the policy with the specified name.

        :param policy_name: Name of the policy to find.
        :return: ID of the found policy or None.
        """
        try:
            policies = self.cluster_policy_api.list_cluster_policies().get('policies', [])
            return next((policy['policy_id'] for policy in policies if policy.get('name') == policy_name), None)
        except Exception as e:
            logging.error(f"Error occurred while listing cluster policies: {e}")
            raise

    def create_or_edit_resource(self, policy_name: str, policy_config: dict):
        """
        Create or edit a cluster policy based on the provided name and configuration.

        :param policy_name: Name of the policy to create or edit.
        :param policy_config: Configuration of the policy.
        """
        try:
            policy_id = self._list_cluster_policies(policy_name)
            if policy_id:
                self.cluster_policy_api.edit_cluster_policy(policy_id, json=policy_config)
                logging.info(f"Cluster policy '{policy_name}' edited successfully.")
            else:
                self.cluster_policy_api.create_cluster_policy(policy_config)
                logging.info(f"Cluster policy '{policy_name}' created successfully.")
        except HTTPError as e:
            logging.error(f"HTTP error during policy '{policy_name}' operation: {e}")
            raise
        except RequestException as req_error:
            logging.error(f"Request error in policy '{policy_name}' operation: {req_error}")
            raise
        except (IOError, OSError) as file_error:
            logging.error(f"File IO error in policy '{policy_name}' operation: {file_error}")
            raise
        except Exception as general_error:
            logging.error(f"General error in policy '{policy_name}' operation: {general_error}")
            raise
