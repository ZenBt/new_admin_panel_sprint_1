from typing import Any
from abc import ABC, abstractmethod
from dataclasses import fields as dataclass_fields

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

        args = ",".join(
            self._cursor.mogrify(f"({', '.join(['%s'] * len(fields))})", item).decode()
            for item in data
        )
        stmt = stmt.format(
            schema=DEFAULT_SCHEMA,
            table=table,
            args=args,
            fields=", ".join(fields),
            unique_fields=", ".join(unique_fields),
        )
        self._cursor.execute(stmt)

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
