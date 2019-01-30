from .. import content_types as content_types


class VirtualGraph(object):
    def __init__(self, name, client):
        self.name = name
        self.client = client

    @property
    def path(self):
        return '/admin/virtual_graphs/{}'.format(self.name)

    def update(self, name, mappings, options):
        meta = {
            'name': name,
            'mappings': mappings,
            'options': options,
        }

        self.client.put(self.path, json=meta)
        self.name = name

    def delete(self):
        self.client.delete(self.path)

    def options(self):
        r = self.client.get(self.path + '/options')
        return r.json()['options']

    def mappings(self, content_type=content_types.TURTLE):
        r = self.client.get(
            self.path + '/mappings', headers={'Accept': content_type})
        return r.content

    def available(self):
        r = self.client.get(self.path + '/available')
        return bool(r.json()['available'])

    def __repr__(self):
        return self.name
