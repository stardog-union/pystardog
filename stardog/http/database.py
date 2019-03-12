class Database(object):
    def __init__(self, name, client):
        self.name = name
        self.client = client
        self.path = '/admin/databases/{}'.format(name)

    def get_options(self, *options):
        # transform into {'option': None} dict
        meta = dict([(x, None) for x in options])

        r = self.client.put(self.path + '/options', json=meta)
        return r.json()

    def set_options(self, options):
        self.client.post(self.path + '/options', json=options)

    def optimize(self):
        self.client.put(self.path + '/optimize')

    def repair(self):
        self.client.put(self.path + '/repair')

    def backup(self, *, to=None):
        params = {'to': to} if to else {}
        self.client.put(self.path + '/backup', params=params)

    def online(self):
        self.client.put(self.path + '/online')

    def offline(self):
        self.client.put(self.path + '/offline')

    def copy(self, to):
        self.client.put(self.path + '/copy', params={'to': to})
        return Database(to, self.client)

    def drop(self):
        self.client.delete(self.path)

    def __repr__(self):
        return self.name
