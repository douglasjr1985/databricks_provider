import logging

from databricks_cli.cluster_policies.api import ClusterPolicyApi
from databricks_cli.sdk.api_client import ApiClient
from requests.exceptions import HTTPError, RequestException


class DatabricksClusterPolicie:
    """
    Manages instance pool operations in Databricks, including listing, creating,
    and editing instance pools.
    """
    def __init__(self, workspace_url: str, client_secret: str):
        """
        Initialize the DatabricksInstancePoolManager with the Databricks workspace URL,
        API token, and the path for configuration files.

        :param workspace_url: URL of the Databricks workspace.
        :param client_secret: API token for authentication.
        """
        self.host = f"https://{workspace_url}/"
        self.token = client_secret
        self.cluster_policie_api = ClusterPolicyApi(ApiClient(host=self.host, token=self.token))

    def _list_cluster_policies(self, policie_name: str):
        """
        List cluster policies and return the ID of the police with the specified name.

        :param policie_name: Name of the police to find.
        :return: ID of the found police or None.
        """
        try:
            policies_response = self.cluster_policie_api.list_cluster_policies()
            policies_list = policies_response.get('policies', [])

            return (any(policy.get('name') == policie_name for policy in policies_list)).get('police_id')
        except Exception as e:
            logging.error(f"Error occurred while searching the police {policie_name} in list of active cluster policies: {e}")
        return None

    def create_or_edit_resource(self, policie_name: str, police_config: dict):
        """
        Create or edit a cluster police based on the provided name and configuration.

        :param policy_name: Name of the policy to create or edit.
        :param policy_config: Configuration of the policy.
        """
        try:
            police_id = self._list_cluster_policies(policie_name)
            if police_id:
                self.cluster_policie_api.edit_cluster_policy(police_id, json=police_config)
                logging.info(f"Cluster policy '{policie_name}' edited successfully.")
            else:
                self.cluster_policie_api.create_cluster_policy(police_config)
                logging.info(f"Cluster policy '{policie_name}' created successfully.")
        except HTTPError as e:
            logging.error(f"HTTP error during policy '{policie_name}' operation: {e}")
        except RequestException as req_error:
            logging.error(f"Request error in policy '{policie_name}' operation: {req_error}")
        except (IOError, OSError) as file_error:
            logging.error(f"File IO error in policy '{policie_name}' operation: {file_error}")
        except Exception as general_error:
            logging.error(f"General error in policy '{policie_name}' operation: {general_error}")