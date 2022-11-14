from .utils import wait_standby_node_to_join, get_node_ip


# We should pass a standby admin object instead of a connection string.
def test_cluster_standby(admin, cluster_standby_node_conn_string):

    with stardog.admin.Admin(**cluster_standby_node_conn_string) as admin_standby:
        assert admin_standby.standby_node_pause(pause=True)
        assert admin_standby.standby_node_pause_status()["STATE"] == "PAUSED"
        assert admin_standby.standby_node_pause(pause=False)
        assert admin_standby.standby_node_pause_status()["STATE"] == "WAITING"

        # Join a standby node is still allowed even if it's not part of the registry.
        admin_standby.cluster_join()
        wait_standby_node_to_join(admin_standby)

        # Make sure the standby node is part of the cluster
        standby_node_info = (
            get_node_ip(STARDOG_HOSTNAME_STANDBY).decode("utf-8").strip() + ":5820"
        )
        assert standby_node_info in admin_standby.cluster_info()["nodes"]

        standby_nodes = admin_standby.cluster_list_standby_nodes()
        node_id = standby_nodes["standbynodes"][0]
        # removes a standby node from the registry, i.e from syncing with the rest of the cluster.
        admin_standby.cluster_revoke_standby_access(standby_nodes["standbynodes"][0])
        standby_nodes_revoked = admin_standby.cluster_list_standby_nodes()
        assert node_id not in standby_nodes_revoked["standbynodes"]
