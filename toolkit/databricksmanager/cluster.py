import logging
from requests.exceptions import HTTPError
from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.clusters.api import ClusterApi

class DatabricksClusterManager:
    """
    Manages cluster operations in Databricks, including listing, creating,
    and editing clusters.
    """

    def __init__(self, workspace_url, api_token):
        """
        Initializes the manager with the workspace URL and API token for Databricks.
        """
        self.host = f"https://{workspace_url}/"
        self.token = api_token
        self.cluster_api = ClusterApi(ApiClient(host=self.host, token=self.token))

    def _list_clusters(self, cluster_name):
        """
        Private method to list clusters and return the ID of the cluster
        with the specified name.
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

    def _create_cluster(self, cluster_config):
        """
        Creates a new cluster with the given configuration.
        """
        try:
            self.cluster_api.create_cluster(cluster_config)
            logging.info("Cluster created successfully.")
        except HTTPError as e:
            logging.error(f"HTTP error during cluster creation: {e}")
            return False
        return True

    def _edit_cluster(self, cluster_id, cluster_config):
        """
        Edits an existing cluster based on the provided cluster ID and configuration.
        """
        try:
            self.cluster_api.edit_cluster(cluster_id, cluster_config)
            logging.info("Cluster edited successfully.")
        except HTTPError as e:
            logging.error(f"HTTP error during cluster editing: {e}")
            return False
        return True

    def create_or_edit_resource(self, cluster_name, cluster_config):
        """
        Creates or edits a cluster based on the provided name and configuration.
        """
        cluster_id = self._list_clusters(cluster_name)
        if cluster_id:
            return self._edit_cluster(cluster_id, cluster_config)
        else:
            return self._create_cluster(cluster_config)
