import json

from . import client as http_client
from . import database as http_database
from . import role as http_role
from . import user as http_user
from . import virtual_graphs as http_virtual_graphs
from . import stored_queries as http_stored_queries


class Admin(object):
    def __init__(self, endpoint=None, username=None, password=None):
        self.client = http_client.Client(endpoint, None, username, password)

    def shutdown(self):
        self.client.post('/admin/shutdown')

    def database(self, name):
        return http_database.Database(name, self.client)

    def databases(self):
        r = self.client.get('/admin/databases')
        return list(map(self.database, r.json()['databases']))

    def new_database(self, name, options=None, *files):
        fmetas = []
        params = []

        for f in files:
            fname = f['name']

            fmeta = {'filename': fname}
            if f.get('context'):
                fmeta['context'] = f['context']

            fmetas.append(fmeta)
            params.append((fname, (fname, f['content'], f['content-type'], {
                'Content-Encoding':
                f.get('content-encoding')
            })))

        meta = {
            'dbname': name,
            'options': options if options else {},
            'files': fmetas
        }

        params.append(('root', (None, json.dumps(meta), 'application/json')))

        self.client.post('/admin/databases', files=params)
        return self.database(name)

    def restore(self, from_path, *, name=None, force=False):
        params = {
            'from': from_path,
            'force': force
        }
        if name:
            params['name'] = name

        self.client.put('/admin/restore', params=params)

    def query(self, id):
        r = self.client.get('/admin/queries/{}'.format(id))
        return r.json()

    def queries(self):
        r = self.client.get('/admin/queries')
        return r.json()['queries']

    def stored_query(self, name):
        return http_stored_queries.StoredQuery(name, self.client)

    def stored_queries(self):
        r = self.client.get('/admin/queries/stored', headers={'Accept': 'application/json'})
        query_names = [q['name'] for q in r.json()['queries']]
        return list(map(self.stored_query, query_names))

    def new_stored_query(self, name, query, options=None):
        if options is None: options = {}
        meta = {
            'name': name,
            'query': query,
            'creator': self.client.username
        }
        meta.update(options)

        self.client.post('/admin/queries/stored', json=meta)
        return self.stored_query(name)

    def clear_stored_queries(self):
        self.client.delete('/admin/queries/stored')

    def kill_query(self, id):
        self.client.delete('/admin/queries/{}'.format(id))

    def user(self, name):
        return http_user.User(name, self.client)

    def users(self):
        r = self.client.get('/admin/users')
        return list(map(self.user, r.json()['users']))

    def new_user(self, username, password, superuser=False):
        meta = {
            'username': username,
            'password': list(password),
            'superuser': superuser,
        }

        self.client.post('/admin/users', json=meta)
        return self.user(username)

    def role(self, name):
        return http_role.Role(name, self.client)

    def roles(self):
        r = self.client.get('/admin/roles')
        return list(map(self.role, r.json()['roles']))

    def new_role(self, name):
        self.client.post('/admin/roles', json={'rolename': name})
        return self.role(name)

    def virtual_graph(self, name):
        return http_virtual_graphs.VirtualGraph(name, self.client)

    def virtual_graphs(self):
        r = self.client.get('/admin/virtual_graphs')
        return list(map(self.virtual_graph, r.json()['virtual_graphs']))

    def new_virtual_graph(self, name, mappings, options):
        meta = {
            'name': name,
            'mappings': mappings,
            'options': options,
        }

        self.client.post('/admin/virtual_graphs', json=meta)
        return self.virtual_graph(name)

    def validate(self):
        self.client.get('/admin/users/valid')

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.client.close()
