from abc import ABC, abstractmethod
from dataclasses import fields as dataclass_fields

from psycopg2.extensions import cursor as _cursor

from dto import (
    PGMovies,
    PGFilmWork,
    PGPerson,
    PGGenre,
    PGGenreFilmwork,
    PGPersonFilmWork,
    RelationalPGMovies,
)

BASE_INSERT_STMT = """
            INSERT INTO {table} ({fields})
            VALUES {args}
            ON CONFLICT ({unique_fields}) DO NOTHING;
            """

FILMWORK_TABLE = "content.film_work"
PERSON_TABLE = "content.person"
GENRE_TABLE = "content.genre"
GENRE_FILMWORK_TABLE = "content.genre_film_work"
PERSON_FILMWORK_TABLE = "content.person_film_work"


FILMWORK_UNIQUE_FIELDS = PERSON_UNIQUE_FIELDS = GENRE_UNIQUE_FIELDS = ("id",)
GENRE_FILMWORK_UNIQUE_FIELDS = ("genre_id", "film_work_id")
PERSON_FILMWORK_UNIQUE_FIELDS = ("id",)


class BasePostgresSaver(ABC):
    def __init__(self, cursor: _cursor):
        self._cursor = cursor

    @abstractmethod
    def save_all_data(self, data: PGMovies | RelationalPGMovies) -> None:
        pass

    def _perform_insert(
        self,
        table: str,
        stmt: str,
        fields: tuple[str, ...],
        unique_fileds: tuple[str, ...],
        items: list,
    ) -> None:

        data = [tuple(getattr(item, field) for field in fields) for item in items]

        args = ",".join(
            self._cursor.mogrify(f"({', '.join(['%s'] * len(fields))})", item).decode()
            for item in data
        )
        stmt = stmt.format(
            table=table,
            args=args,
            fields=", ".join(fields),
            unique_fields=", ".join(unique_fileds),
        )
        self._cursor.execute(stmt)

    def _get_dataclass_fields(self, dataclass: type) -> tuple[str, ...]:
        return tuple(field.name for field in dataclass_fields(dataclass))


class PostgresSaver(BasePostgresSaver):
    def save_all_data(self, data: PGMovies):
        self._save_filmworks(data.filmworks)
        self._save_genres(data.genres)
        self._save_persons(data.persons)

    def _save_filmworks(self, filmworks: list[PGFilmWork]):
        if not filmworks:
            return

        self._perform_insert(
            FILMWORK_TABLE,
            BASE_INSERT_STMT,
            self._get_dataclass_fields(PGFilmWork),
            FILMWORK_UNIQUE_FIELDS,
            filmworks,
        )

    def _save_genres(self, genres: list[PGGenre]):
        if not genres:
            return

        self._perform_insert(
            GENRE_TABLE,
            BASE_INSERT_STMT,
            self._get_dataclass_fields(PGGenre),
            GENRE_UNIQUE_FIELDS,
            genres,
        )

    def _save_persons(self, persons: list[PGPerson]):
        if not persons:
            return

        self._perform_insert(
            PERSON_TABLE,
            BASE_INSERT_STMT,
            self._get_dataclass_fields(PGPerson),
            PERSON_UNIQUE_FIELDS,
            persons,
        )


class RelationalPostgresSaver(BasePostgresSaver):
    def save_all_data(self, data: RelationalPGMovies):
        self._save_genre_film_works(data.genre_film_works)
        self._save_person_film_works(data.person_film_works)

    def _save_genre_film_works(self, genre_film_works: list[PGGenreFilmwork]):
        if not genre_film_works:
            return

        self._perform_insert(
            GENRE_FILMWORK_TABLE,
            BASE_INSERT_STMT,
            self._get_dataclass_fields(PGGenreFilmwork),
            GENRE_FILMWORK_UNIQUE_FIELDS,
            genre_film_works,
        )

    def _save_person_film_works(self, person_film_works: list[PGPersonFilmWork]):
        if not person_film_works:
            return

        self._perform_insert(
            PERSON_FILMWORK_TABLE,
            BASE_INSERT_STMT,
            self._get_dataclass_fields(PGPersonFilmWork),
            PERSON_FILMWORK_UNIQUE_FIELDS,
            person_film_works,
        )
