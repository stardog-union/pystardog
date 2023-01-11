import stardog


def test_import_stardog_all_shortcut(conn_string):
    with stardog.Admin(**conn_string) as admin:
        assert admin.healthcheck()


def test_import_stardog_full_path(conn_string):
    with stardog.admin.Admin(**conn_string) as admin:
        assert admin.healthcheck()
