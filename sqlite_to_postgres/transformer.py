from typing import Any, Protocol

from dto import DTO_TABLES_MAPPING


class BaseTransformer(Protocol):
    def __init__(self, data: dict, table: str) -> None:
        ...

    def transform(self) -> Any:
        pass


class SQLiteToPGTransformer(BaseTransformer):
    def __init__(self, rows: list[dict], table: str) -> None:
        self._table = table
        self._rows = rows

    def transform(self) -> list[Any]:
        """Transform SQLite data to Postgres data"""
        rows = self._transform_data()
        return [DTO_TABLES_MAPPING[self._table](**row) for row in rows]

    def _transform_data(self):
        for row in self._rows:
            if created := row.get("created_at"):
                row["created"] = created
                del row["created_at"]

            if modified := row.get("updated_at"):
                row["modified"] = modified
                del row["updated_at"]

        return self._rows
