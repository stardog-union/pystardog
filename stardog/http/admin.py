import json

from stardog.http.client import Client
from stardog.http.database import Database
from stardog.http.role import Role
from stardog.http.user import User
from stardog.http.virtual_graphs import VirtualGraph


class Admin(object):

    def __init__(self, endpoint=None, username=None, password=None):
        self.client = Client(endpoint, None, username, password)

    def shutdown(self):
        self.client.post('/admin/shutdown')

    def database(self, name):
        return Database(name, self.client)

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
            params.append((fname, (fname, f['content'], f['content-type'])))

        meta = {
            'dbname': name,
            'options': options if options else {},
            'files': fmetas
        }

        params.append(('root', (None, json.dumps(meta), 'application/json')))

        self.client.post('/admin/databases', files=params)
        return self.database(name)

    def query(self, id):
        r = self.client.get('/admin/queries/{}'.format(id))
        return r.json()

    def queries(self):
        r = self.client.get('/admin/queries')
        return r.json()['queries']

    def kill_query(self, id):
        self.client.delete('/admin/queries/{}'.format(id))

    def user(self, name):
        return User(name, self.client)

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
        return Role(name, self.client)

    def roles(self):
        r = self.client.get('/admin/roles')
        return list(map(self.role, r.json()['roles']))

    def new_role(self, name):
        self.client.post('/admin/roles', json={'rolename': name})
        return self.role(name)

    def virtual_graph(self, name):
        return VirtualGraph(name, self.client)

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
