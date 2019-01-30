from .. import content_types as content_types


class VCS(object):
    def __init__(self, conn):
        self.client = conn.client

    def query(self, query, content_type=content_types.SPARQL_JSON, **kwargs):
        params = {
            'query': query,
            'baseURI': kwargs.get('base_uri'),
            'limit': kwargs.get('limit'),
            'offset': kwargs.get('offset'),
            'timeout': kwargs.get('timeout'),
            'reasoning': kwargs.get('reasoning')
        }

        # query bindings
        bindings = kwargs.get('bindings', {})
        for k, v in bindings.items():
            params['${}'.format(k)] = v

        r = self.client.post(
            '/vcs/query',
            data=params,
            headers={'Accept': content_type},
        )

        return r.json(
        ) if content_type == content_types.SPARQL_JSON else r.content

    def commit(self, transaction, message):
        self.client.post('/vcs/{}/commit_msg'.format(transaction))

    def create_tag(self, revision, name):
        self.client.post(
            '/vcs/tags/create', data='"{}", "{}"'.format(revision, name))

    def delete_tag(self, name):
        self.client.post('/vcs/tags/delete', data=name)

    def revert(self, to_revision, from_revision, message):
        self.client.post(
            '/vcs/revert',
            data='"{}", "{}", "{}"'.format(to_revision, from_revision,
                                           message))
