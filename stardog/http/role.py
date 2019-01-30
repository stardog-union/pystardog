from . import user as http_user


class Role(object):
    def __init__(self, name, client):
        self.name = name
        self.client = client
        self.path = '/admin/roles/{}'.format(name)

    def users(self):
        r = self.client.get(self.path + '/users')
        return [
            http_user.User(name, self.client) for name in r.json()['users']
        ]

    def delete(self, force=None):
        self.client.delete(self.path, params={'force': force})

    def permissions(self):
        r = self.client.get('/admin/permissions/role/{}'.format(self.name))
        return r.json()['permissions']

    def add_permission(self, action, resource_type, resource):
        meta = {
            'action': action,
            'resource_type': resource_type,
            'resource': [resource]
        }

        self.client.put(
            '/admin/permissions/role/{}'.format(self.name), json=meta)

    def remove_permission(self, action, resource_type, resource):
        meta = {
            'action': action,
            'resource_type': resource_type,
            'resource': [resource]
        }

        self.client.post(
            '/admin/permissions/role/{}/delete'.format(self.name), json=meta)

    def __repr__(self):
        return self.name
