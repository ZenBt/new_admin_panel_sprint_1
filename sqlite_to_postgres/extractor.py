from sqlite3 import Connection as SQLiteConnection


class SQLiteExtractor:
    def __init__(self, connection: SQLiteConnection) -> None:
        self._conn = connection
