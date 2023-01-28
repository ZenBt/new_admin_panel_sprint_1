from psycopg2.extensions import connection as _connection


class PostgresSaver:
    def __init__(self, connection: _connection):
        self._conn = connection
