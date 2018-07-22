import BaseHTTPServer
import functools
import json
import threading


class HTTPError(Exception):
    def __init__(self, code, reason):
        super(HTTPError, self).__init__(reason)
        self.code = code


class JsonSerializable(object):
    """Implements method toDict to convert class to dict.

    Inherited class should use __slots__
    """

    def to_dict(self):
        return {
            k: getattr(self, k) for k in self.__slots__
        }


class JSONEncoder(json.JSONEncoder):
    """The custom json encoder, which allows to serialize JsonSerializable objects"""
    def default(self, o):
        if isinstance(o, JsonSerializable):
            return o.to_dict()
        return super(JSONEncoder, self).default(o)


class UserSchema(object):
    """The class to validate correctness of input data for User methods"""

    schema = {
        'age': (int, lambda x: 0 < x < 100),
        'name': (basestring, lambda x: x),
        'email': (basestring, lambda x: x),
        'sex': (basestring, lambda x: x in ('male', 'female')),
    }

    @classmethod
    def check_value(cls, name, value):
        try:
            traits = cls.schema[name]
        except KeyError:
            raise HTTPError(400, 'unknown value {}'.format(name))

        if not isinstance(value, traits[0]):
            raise HTTPError(400, '{} does has wrong type, expected {}'.format(name, traits[0]))
        if not traits[1](value):
            raise HTTPError(400, '{} does has wrong value'.format(name))

    @classmethod
    def validate(cls, d):
        for k, v in d.items():
            cls.check_value(k, v)


class User(JsonSerializable):
    """The resource - user"""

    __slots__ = ('id', 'name', 'email', 'age', 'sex')

    def __init__(self, id_, name, email, age, sex):
        self.id = id_
        self.name = name
        self.email = email
        self.age = age
        self.sex = sex

    def update(self, d):
        """Updates user from dict"""
        for k, v in d.items():
            setattr(self, k, v)


class UserRef(JsonSerializable):
    """Reference to user, contains minimal number of fields to identify user."""

    __slots__ = ('id', 'name', 'url')

    def __init__(self, user):
        self.id = user.id
        self.name = user.name
        self.url = '/users/{}'.format(user.id)


class UserController(object):
    """Manages Users collection."""

    schema = UserSchema

    def __init__(self):
        self.users = {}
        self.last_id = 1
        self.emails = set()
        self.lock = threading.Lock()

    def __user_id_from_str(self, id_str):
        """Parse user id from string."""
        try:
            return int(id_str)
        except ValueError:
            raise HTTPError(404, 'users with id {} is not found'.format(id_str))

    def __next_id(self):
        """Return the next unique id for user."""
        with self.lock:
            id_ = self.last_id
            self.last_id += 1
            return id_

    def __update_email(self, new_email, old_email):
        """Updates unique emails index."""
        if new_email == old_email:
            return

        if new_email is not None:
            if new_email in self.emails:
                raise HTTPError(409, 'Email is not unique')
            self.emails.add(new_email)

        if old_email is not None:
            self.emails.remove(old_email)

    def create(self, data):
        """Creates a new user."""
        # validate first
        self.schema.validate(data)

        id_ = self.__next_id()
        data['id_'] = id_

        user = User(**data)
        with self.lock:
            self.__update_email(user.email, None)

        self.users[id_] = user
        return UserRef(user)

    def list(self):
        """Get list of users, (sorted by id)."""
        users_ids = sorted(self.users)
        users_refs = []
        for id_ in users_ids:
            try:
                users_refs.append(self.users[id_])
            except KeyError:
                pass

        return users_refs

    def get(self, id_str):
        """Get user by id."""
        id_ = self.__user_id_from_str(id_str)
        try:
            return self.users[id_]
        except KeyError:
            raise HTTPError(404, 'user({}) is not found'.format(id_))

    def update(self, id_str, data):
        """Update user by id."""
        user = self.get(id_str)
        self.schema.validate(data)
        with self.lock:
            self.__update_email(data.get('email'), user.email)
            user.update(data)
        return user

    def delete(self, id_str):
        """Delete users by id."""
        user = self.get(id_str)
        with self.lock:
            self.__update_email(None, user.email)
        del self.users[user.id]


class HTTPHandler(BaseHTTPServer.BaseHTTPRequestHandler):

    """The main http handler, routes requests by path and calls appropriate controller methods."""

    controller = UserController()

    def do_GET(self):
        """Process GET requests."""
        if self.path == '/users/':
            return self.process_request(200, self.controller.list)

        if self.path.startswith('/users/'):
            # /users/{id}
            # '/users/1'.split('/') -> ['', 'users', '1']
            parts = self.path.split('/', 4)
            if 3 == len(parts):
                user_id = parts[2]
                return self.process_request(200, functools.partial(self.controller.get, user_id))

        self.not_found()

    def do_POST(self):
        """Process POST requests."""
        if self.path == '/users/':
            return self.process_request(201, functools.partial(self.call_with_body, self.controller.create))
        self.not_found()

    def do_PUT(self):
        """Process PUT requests."""
        user_id = self.get_user_id()
        if user_id is not None:
            update_user = functools.partial(self.controller.update, user_id)
            return self.process_request(200, functools.partial(self.call_with_body, update_user))

        self.not_found()

    def do_DELETE(self):
        """Process DELETE requests."""
        user_id = self.get_user_id()
        if user_id is not None:
            return self.process_request(200, functools.partial(self.controller.delete, user_id))

        self.not_found()

    def get_user_id(self):
        """Extract user id from request path."""
        if self.path.startswith('/users/'):
            parts = self.path.split('/', 4)
            if 3 == len(parts):
                return parts[2]

    def get_data(self):
        """Get request params from body"""
        if not self.headers['Content-Type'].startswith('application/json'):
            raise HTTPError(415, 'expected application/json')

        number_of_bits = int(self.headers['Content-Length'])
        body = self.rfile.read(number_of_bits)
        return json.loads(body, encoding='utf-8')

    def call_with_body(self, handler):
        """Call handler with request body."""

        try:
            data = self.get_data()
        except Exception as e:
            raise HTTPError(400, str(e))

        return handler(data)

    def process_request(self, status, handler):
        """Process requests and handle exceptions"""
        try:
            data = handler()
        except HTTPError as e:
            data = {'error': str(e)}
            status = e.code
        except Exception as e:
            data = {'error': str(e)}
            status = 500

        self.write_response(status, data)

    def write_response(self, status, data):
        """Formats response as json and writes"""
        if data is not None:
            body = json.dumps(data, sort_keys=True, indent=4, cls=JSONEncoder).encode('utf-8')
            self.send_response(status)
            self.send_header('Content-Type', 'application/json; charset=utf=8')
            self.send_header('Content-Length',  len(body))
            self.end_headers()
            self.wfile.write(body)
            self.wfile.flush()
        else:
            self.send_response(204)

    def not_found(self):
        self.write_response(404, {'error': 'not found'})


def main():
    server_address = ('127.0.0.1', 8080)
    server = BaseHTTPServer.HTTPServer(server_address, HTTPHandler)
    server.serve_forever()


if __name__ == '__main__':
    main()
