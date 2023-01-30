from typing import Any, Protocol

from dto import DTO_TABLES_MAPPING


class BaseTransformer(Protocol):
    def __init__(self, data: dict, table: str) -> None:
        ...

    def transform(self) -> Any:
        pass


class SQLiteToPGTransformer(BaseTransformer):
    def __init__(self, data: dict, table: str) -> None:
        self._table = table
        self._data = data

    def transform(self) -> Any:
        """Transform SQLite data to Postgres data"""
        data = self._transform_data(data=self._data)
        return DTO_TABLES_MAPPING[self._table](**data)

    def _transform_data(self, data: dict):
        if created := data.get("created_at"):
            data["created"] = created
            del data["created_at"]

        if modified := data.get("updated_at"):
            data["modified"] = modified
            del data["updated_at"]

        return data
