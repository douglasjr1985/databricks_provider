import logging

from requests.exceptions import HTTPError, RequestException
from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.clusters.api import ClusterApi

class DatabricksClusterManager:
    """
    Manages cluster operations in Databricks, including listing, creating,
    and editing clusters.
    """
    def __init__(self, workspace_url, client_secret):
        """
        Initialize the DatabricksClusterManager with the Databricks workspace URL,
        API token, and the path for configuration files.
        """
        self.host = f"https://{workspace_url}/"
        self.token = client_secret
        self.cluster_api = ClusterApi(ApiClient(host=self.host, token=self.token))

    def _list_clusters(self, cluster_name: str):
        """
        List clusters and return the ID of the cluster with the specified name.
        Private method used internally.
        """        
        try:
            clusters = self.cluster_api.list_clusters()
            for cluster in clusters.get('clusters', []):
                if cluster['cluster_name'] == cluster_name:
                    return cluster['cluster_id']
        except HTTPError as e:
            logging.error(f"HTTP error occurred: {e}")
        except Exception as e:
            logging.error(f"Unexpected error occurred: {e}")
        return None

    def create_or_edit_resource(self, cluster_name: str, cluster_config: dict):
        """
        Create or edit an cluster  based on the provided name and configuration.
        Public method to be called externally.
        """
        try:
            cluster_id = self._list_clusters(cluster_name)
            if cluster_id:
                self.cluster_api.edit_cluster(cluster_config)
                logging.info(f"Cluster '{cluster_id}' edited successfully.")
            else:
                self.cluster_api.create_cluster(cluster_config)
                logging.info(f"Cluster '{cluster_id}' created successfully.")
        except HTTPError as e:
            logging.error(f"HTTP error during cluster '{cluster_name}': {e}")
            
        except RequestException as req_error:
            logging.error(f"HTTP request error in creating/editing cluster '{cluster_name}': {req_error}")
                        
        except (IOError, OSError) as file_error:
            logging.error(f"File IO error in creating/editing cluster '{cluster_name}': {file_error}")
                        
        except Exception as general_error:
            logging.error(f"General error in creating/editing cluster '{cluster_name}': {general_error}")
                           