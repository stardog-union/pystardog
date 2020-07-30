from .. import content_types as content_types


class StoredQuery(object):
    def __init__(self, name, client):
        self.name = name
        self.client = client
        self.__refresh()

    def __refresh(self):
        details = self.client.get(self.path, headers={'Accept': 'application/json'})
        self.__dict__.update(details.json()['queries'][0])

    @property
    def path(self):
        return '/admin/queries/stored/{}'.format(self.name)

    def delete(self):
        self.client.delete(self.path)

    def update(self, **options):
        options['name'] = self.name
        for opt in ['query', 'creator']:
            if not opt in options: options[opt] = self.__getattribute__(opt)

        self.client.put('/admin/queries/stored', json=options)
        self.__refresh()

    def __repr__(self):
        return self.name
