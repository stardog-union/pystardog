from contextlib2 import ExitStack

from stardog.http.admin import Admin as HTTPAdmin

class Admin(object):

    def __init__(self, endpoint=None, username=None, password=None):
        self.admin = HTTPAdmin(endpoint, username, password)
    
    def shutdown(self):
        self.admin.shutdown()
    
    def database(self, name):
        return self.admin.database(name)

    def databases(self):
        return self.admin.databases()

    def new_database(self, name, options=None, contents=None):
        contents = contents if contents else []
        files = []

        with ExitStack() as stack:
            for c in contents:
                content = c[0] if isinstance(c, tuple) else c
                context = c[1] if isinstance(c, tuple) else None
                
                # we will be opening references to many sources in a single call
                # use a stack manager to make sure they all get properly closed at the end
                data = stack.enter_context(content.data())

                files.append({
                    'name': content.name,
                    'content': data,
                    'content-type': content.content_type,
                    'context': context
                })
            
            return self.admin.new_database(name, options, files)

    def query(self, id):
        return self.admin.query(id)

    def queries(self):
        return self.admin.queries()

    def kill_query(self, id):
        return self.admin.kill_query(id)

    def user(self, name):
        return self.admin.user(name)

    def users(self):
        return self.admin.users()

    def new_user(self, username, password, superuser=False):
        return self.admin.new_user(username, password, superuser)

    def role(self, name):
        return self.admin.role(name)

    def roles(self):
        return self.admin.roles()

    def new_role(self, name):
        return self.admin.new_role(name)

    def virtual_graph(self, name):
        return self.admin.virtual_graph(name)

    def virtual_graphs(self):
        return self.admin.virtual_graphs()

    def new_virtual_graph(self, name, mappings, options):
        return self.admin.new_virtual_graph(name, mappings, options)

    def validate(self):
        return self.admin.validate()
