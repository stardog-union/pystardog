from stardog.content_types import SPARQL_JSON

class VCS(object):

    def __init__(self, conn):
        self.client = conn.client
    
    def query(self, query, base_uri=None, limit=None, offset=None, timeout=None, reasoning=False, bindings=None, content_type=SPARQL_JSON):
        params = {
            'query': query,
            'baseURI': base_uri,
            'limit': limit,
            'offset': offset,
            'timeout': timeout,
            'reasoning': reasoning
        }

        # query bindings
        bindings = bindings if bindings else {}
        for k,v in bindings.iteritems():
            params['${}'.format(k)] = v

        r = self.client.post(
            '/vcs/query',
            data=params,
            headers={'Accept': content_type},
        )

        return r.json() if content_type == SPARQL_JSON else r.content

    def commit(self, transaction, message):
        self.client.post(
            '/vcs/{}/commit_msg'.format(transaction)
        )
    
    def create_tag(self, revision, name):
        self.client.post(
            '/vcs/tags/create',
            data='"{}", "{}"'.format(revision, name)
        )

    def delete_tag(self, name):
        self.client.post(
            '/vcs/tags/delete',
            data=name
        )

    def revert(self, to_revision, from_revision, message):
        self.client.post(
            '/vcs/revert',
            data='"{}", "{}", "{}"'.format(to_revision, from_revision, message)
        )
