from stardog.role import Role

class User(object):

    def __init__(self, name, client):
        self.name = name
        self.client = client
        self.path = '/admin/users/{}'.format(name)

    def set_password(self, password):
        self.client.put(self.path + '/pwd', json={'password': password})

    def is_enabled(self):
        r = self.client.get(self.path + '/enabled')
        return r.json()['enabled']

    def set_enabled(self, enabled):
        self.client.put(self.path + '/enabled', json={'enabled': enabled})
    
    def is_superuser(self):
        r = self.client.get(self.path + '/superuser')
        return r.json()['superuser']

    def roles(self):
        r = self.client.get(self.path + '/roles')
        return map(lambda name: Role(name, self.client), r.json()['roles'])

    def add_role(self, role):
        self.client.post(self.path + '/roles', json={'rolename': self.__rolename(role)})
    
    def set_roles(self, *roles):
        self.client.put(self.path + '/roles', json={'roles': map(self.__rolename, roles)})

    def remove_role(self, role):
        self.client.delete(self.path + '/roles/' + self.__rolename(role))
    
    def delete(self):
        self.client.delete(self.path)

    def permissions(self):
        r = self.client.get('/admin/permissions/user/{}'.format(self.name))
        return r.json()['permissions']

    def add_permission(self, action, resource_type, resource):
        meta = {
            'action': action,
            'resource_type': resource_type,
            'resource': [resource]
        }
        self.client.put('/admin/permissions/user/{}'.format(self.name), json=meta)

    def remove_permission(self, action, resource_type, resource):
        meta = {
            'action': action,
            'resource_type': resource_type,
            'resource': [resource]
        }

        self.client.post('/admin/permissions/user/{}/delete'.format(self.name), json=meta)

    def effective_permissions(self):
        r = self.client.get('/admin/permissions/effective/user/' + self.name)
        return r.json()['permissions']

    def __rolename(self, role):
        return role.name if isinstance(role, Role) else role

    def __repr__(self):
        return self.name