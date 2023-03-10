import logging
from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import fields as dataclass_fields
from typing import Any

import psycopg2
from psycopg2 import extras
from psycopg2.extensions import connection as _connection
from psycopg2.extensions import cursor as _cursor

from dto import DTO_TABLES_MAPPING

DEFAULT_SCHEMA = "content"
BASE_INSERT_STMT = """
            INSERT INTO {schema}.{table} ({fields})
            VALUES {args}
            ON CONFLICT ({unique_fields}) DO NOTHING;
            """

ID_UNIQUE_FIELDS = ("id",)
GENRE_FILMWORK_UNIQUE_FIELDS = ("genre_id", "film_work_id")


class BasePostgresSaver(ABC):
    def __init__(self, cursor: _cursor):
        self._cursor = cursor

    @abstractmethod
    def save(self, table: str, data: list[Any]) -> None:
        pass

    def _perform_insert(
        self,
        table: str,
        stmt: str,
        fields: tuple[str, ...],
        unique_fields: tuple[str, ...],
        items: list,
    ) -> None:

        data = [tuple(getattr(item, field) for field in fields) for item in items]

        args = f"({', '.join(['%s'] * len(fields))})"
            
        stmt = stmt.format(
            schema=DEFAULT_SCHEMA,
            table=table,
            args=args,
            fields=", ".join(fields),
            unique_fields=", ".join(unique_fields),
        )
        extras.execute_batch(self._cursor, stmt, data)

    def _get_dataclass_fields(self, dataclass: type) -> tuple[str, ...]:
        return tuple(field.name for field in dataclass_fields(dataclass))


class PostgresSaver(BasePostgresSaver):
    def save(self, table: str, items: list[Any]) -> None:
        unique_fields = (
            ID_UNIQUE_FIELDS
            if table != "genre_film_work"
            else GENRE_FILMWORK_UNIQUE_FIELDS
        )
        self._perform_insert(
            table=table,
            stmt=BASE_INSERT_STMT,
            fields=self._get_dataclass_fields(DTO_TABLES_MAPPING[table]),
            unique_fields=unique_fields,
            items=items,
        )


@contextmanager
def connect_to_postgres(DSL: dict, cursor_factory: Any) -> _cursor:
    pg_conn: _connection = psycopg2.connect(**DSL, cursor_factory=cursor_factory)
    try:
        logging.debug("Creating connection")
        with pg_conn.cursor() as pg_cur:
            logging.debug("Creating cursor")
            yield pg_cur

    finally:
        logging.debug("Closing connection")
        pg_conn.commit()
        pg_conn.close()
