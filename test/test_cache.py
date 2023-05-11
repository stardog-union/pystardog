import pytest


def test_cache_targets(admin, cache_target_info):

    cache_target_name = cache_target_info["target_name"]
    cache_target_hostname = cache_target_info["hostname"]
    cache_target_port = cache_target_info["port"]
    cache_target_username = cache_target_info["username"]
    cache_target_password = cache_target_info["password"]

    cache_targets = admin.cache_targets()
    assert len(cache_targets) == 0

    cache_target = admin.new_cache_target(
        cache_target_name,
        cache_target_hostname,
        cache_target_port,
        cache_target_username,
        cache_target_password,
    )
    wait_for_creating_cache_target(admin, cache_target_name)
    cache_targets = admin.cache_targets()
    assert len(cache_targets) == 1

    assert cache_target.info()["name"] == cache_target_name
    assert cache_target.info()["port"] == cache_target_port
    assert cache_target.info()["hostname"] == cache_target_hostname
    assert cache_target.info()["username"] == cache_target_username

    # test remove()
    cache_target.remove()
    wait_for_cleaning_cache_target(admin, cache_target.name)
    cache_targets = admin.cache_targets()
    assert len(cache_targets) == 0

    # tests orphan
    cache_target = admin.new_cache_target(
        cache_target_name,
        cache_target_hostname,
        cache_target_port,
        cache_target_username,
        cache_target_password,
    )
    wait_for_creating_cache_target(admin, cache_target_name)
    cache_target.orphan()
    wait_for_cleaning_cache_target(admin, cache_target.name)
    cache_targets = admin.cache_targets()
    assert len(cache_targets) == 0
    # recall that removing a cache is an idempotent operation, and will always (unless it fails)
    # return that cache target was removed, even if it doesn't exists in the first place
    # Removing a an orphaned cache target will not delete the data, because the target is already orphaned
    # We need to recreate the orphaned cached target (using use_existing_db) in order to fully delete its data
    cache_target = admin.new_cache_target(
        cache_target_name,
        cache_target_hostname,
        cache_target_port,
        cache_target_username,
        cache_target_password,
        use_existing_db=True,
    )
    wait_for_creating_cache_target(admin, cache_target_name)
    cache_target.remove()


def test_cache_ng_datasets(admin, bulkload_content, cache_target_info):

    cache_target_name = cache_target_info["target_name"]
    cache_target_hostname = cache_target_info["hostname"]
    cache_target_port = cache_target_info["port"]
    cache_target_username = cache_target_info["username"]
    cache_target_password = cache_target_info["password"]

    cache_target = admin.new_cache_target(
        cache_target_name,
        cache_target_hostname,
        cache_target_port,
        cache_target_username,
        cache_target_password,
    )
    wait_for_creating_cache_target(admin, cache_target_name)

    bl = admin.new_database("bulkload", {}, *bulkload_content, copy_to_server=True)

    assert len(admin.cached_graphs()) == 0
    cached_graph_name = "cache://cached-ng"
    cached_graph = admin.new_cached_graph(
        cached_graph_name, cache_target.name, "urn:context", bl.name
    )

    assert len(admin.cached_graphs()) == 1

    cached_graph_status = cached_graph.status()
    assert cached_graph_status[0]["name"] == cached_graph_name
    assert cached_graph_status[0]["target"] == cache_target.name
    cached_graph.refresh()
    cached_graph.drop()
    assert len(admin.cached_graphs()) == 0


def test_cache_vg_datasets(admin, music_options, cache_target_info):

    cache_target_name = cache_target_info["target_name"]
    cache_target_hostname = cache_target_info["hostname"]
    cache_target_port = cache_target_info["port"]
    cache_target_username = cache_target_info["username"]
    cache_target_password = cache_target_info["password"]

    # creating a VG using empty mappings, and specifying a datasource
    ds = admin.new_datasource("music", music_options)
    vg = admin.new_virtual_graph("test_vg", mappings="", datasource=ds.name)

    cache_target = admin.new_cache_target(
        cache_target_name,
        cache_target_hostname,
        cache_target_port,
        cache_target_username,
        cache_target_password,
    )
    wait_for_creating_cache_target(admin, cache_target_name)
    # We need to register the VG into the cache target as well.

    conn_string_cache = {
        "endpoint": "http://" + cache_target_hostname + ":5820",
        "username": "admin",
        "password": "admin",
    }

    with stardog.admin.Admin(**conn_string_cache) as admin_cache_target:
        ds_cached = admin_cache_target.new_datasource("music", music_options)
        vg_cached = admin_cache_target.new_virtual_graph(
            "test_vg", mappings="", datasource=ds.name
        )

    assert len(admin.cached_graphs()) == 0

    cached_graph_name = "cache://cached-vg"
    cached_graph = admin.new_cached_graph(
        cached_graph_name, cache_target.name, "virtual://" + vg.name
    )

    assert len(admin.cached_graphs()) == 1

    cached_graph_status = cached_graph.status()
    assert cached_graph_status[0]["name"] == cached_graph_name
    assert cached_graph_status[0]["target"] == cache_target.name
    cached_graph.refresh()
    cached_graph.drop()
    assert len(admin.cached_graphs()) == 0

    cache_target.remove()
    wait_for_cleaning_cache_target(admin, cache_target.name)

    vg.delete()
    ds.delete()


@pytest.mark.skip(
    reason="Caching queries is no longer supported. We are skipping we but should make sure it still works for older SD versions"
)
def test_cache_query_datasets(admin, bulkload_content, cache_target_info):

    cache_target_name = cache_target_info["target_name"]
    cache_target_hostname = cache_target_info["hostname"]
    cache_target_port = cache_target_info["port"]
    cache_target_username = cache_target_info["username"]
    cache_target_password = cache_target_info["password"]

    cache_target = admin.new_cache_target(
        cache_target_name,
        cache_target_hostname,
        cache_target_port,
        cache_target_username,
        cache_target_password,
    )
    wait_for_creating_cache_target(admin, cache_target_name)

    bl = admin.new_database("bulkload", {}, *bulkload_content, copy_to_server=True)

    assert len(admin.cached_queries()) == 0

    cached_query_name = "cache://my_new_query_cached"

    cached_query = admin.new_cached_query(
        cached_query_name, cache_target.name, "SELECT * { ?s ?p ?o }", bl.name
    )
    # wait_for_creating_cache_dataset(admin, cached_query_name, 'query')

    assert len(admin.cached_queries()) == 1

    cached_query_status = cached_query.status()
    assert cached_query_status[0]["name"] == cached_query_name
    assert cached_query_status[0]["target"] == cache_target.name
    cached_query.refresh()
    cached_query.drop()
    assert len(admin.cached_queries()) == 0

    cache_target.remove()
    wait_for_cleaning_cache_target(admin, cache_target.name)


def test_cache_target_in_cache_target_list(admin, cache_target_info):
    cache_target_name = cache_target_info["target_name"]
    cache_target_hostname = cache_target_info["hostname"]
    cache_target_port = cache_target_info["port"]
    cache_target_username = cache_target_info["username"]
    cache_target_password = cache_target_info["password"]

    cache_targets = admin.cache_targets()
    assert len(cache_targets) == 0

    cache_target = admin.new_cache_target(
        cache_target_name,
        cache_target_hostname,
        cache_target_port,
        cache_target_username,
        cache_target_password,
    )
    wait_for_creating_cache_target(admin, cache_target_name)
    assert cache_target in admin.cache_targets()
