from abc import ABC, abstractmethod

from dto import (
    SQLiteMovies,
    PGMovies,
    PGFilmWork,
    PGPerson,
    PGGenre,
    PGGenreFilmwork,
    PGPersonFilmWork,
    RelationalSQLiteMovies,
    RelationalPGMovies,
)

class BaseTransformer(ABC):
    @abstractmethod
    def transform(self) -> PGMovies | RelationalPGMovies:
        pass

    @property
    def sqlite_data(self) -> SQLiteMovies | RelationalSQLiteMovies:
        return self._sqlite_data

    @sqlite_data.setter
    def sqlite_data(self, value: SQLiteMovies | RelationalSQLiteMovies) -> None:
        self._sqlite_data = value


class SQLiteToPGTransformer(BaseTransformer):
    
    def __init__(self):
        self._sqlite_data: SQLiteMovies | None = None

    def transform(self) -> PGMovies:
        """Transform SQLite data to Postgres data

        :return: Postgres data
        :raises AssertionError: if SQLite data is not set
        """
        assert self.sqlite_data is not None, "SQLite data is not set"

        filmworks = self._transform_filmworks()
        genres = self._transform_genres()
        persons = self._transform_persons()

        return self._make_pg_data(
            filmworks=filmworks,
            genres=genres,
            persons=persons,
        )

    def _transform_persons(self) -> list[PGPerson]:
        return [
            PGPerson(
                id=person.id,
                full_name=person.full_name,
                created=person.created_at,
                modified=person.updated_at,
            )
            for person in self.sqlite_data.persons
        ]

    def _transform_filmworks(self) -> list[PGFilmWork]:
        return [
            PGFilmWork(
                id=filmwork.id,
                title=filmwork.title,
                description=filmwork.description,
                creation_date=filmwork.creation_date,
                rating=filmwork.rating,
                type=filmwork.type,
                created=filmwork.created_at,
                modified=filmwork.updated_at,
            )
            for filmwork in self.sqlite_data.film_works
        ]

    def _transform_genres(self) -> list[PGGenre]:
        return [
            PGGenre(
                id=genre.id,
                name=genre.name,
                description=genre.description,
                created=genre.created_at,
                modified=genre.updated_at,
            )
            for genre in self.sqlite_data.genres
        ]


    def _make_pg_data(
        self,
        filmworks: list[PGFilmWork],
        genres: list[PGFilmWork],
        persons: list[PGPerson],
    ) -> PGMovies:
        return PGMovies(
            filmworks=filmworks,
            genres=genres,
            persons=persons,
        )


class RelationalSQLiteToPGTransformer(BaseTransformer):
    def __init__(self):
        self._sqlite_data: RelationalSQLiteMovies | None = None

    def transform(self) -> PGMovies:
        """Transform SQLite data to Postgres data

        :return: Postgres data
        :raises AssertionError: if SQLite data is not set
        """
        assert (
            self.sqlite_data is not None
        ), "SQLite data with relations is not set"


        genre_film_works = self._transform_genre_film_works()
        person_film_works = self._transform_person_film_works()

        return self._make_pg_data(
            genre_film_works=genre_film_works,
            person_film_works=person_film_works,
        )
    
    def _transform_genre_film_works(self) -> list[PGGenreFilmwork]:
        return [
            PGGenreFilmwork(
                id=genre_filmwork.id,
                genre_id=genre_filmwork.genre_id,
                film_work_id=genre_filmwork.film_work_id,
                created=genre_filmwork.created_at,
            )
            for genre_filmwork in self.sqlite_data.genre_film_works
        ]

    def _transform_person_film_works(self) -> list[PGPersonFilmWork]:
        return [
            PGPersonFilmWork(
                id=person_filmwork.id,
                person_id=person_filmwork.person_id,
                film_work_id=person_filmwork.film_work_id,
                role=person_filmwork.role,
                created=person_filmwork.created_at,
            )
            for person_filmwork in self.sqlite_data.person_film_works
        ]
    
    def _make_pg_data(
        self,
        genre_film_works: list[PGGenreFilmwork],
        person_film_works: list[PGPersonFilmWork],
    ) -> RelationalPGMovies:
        return RelationalPGMovies(
            genre_film_works=genre_film_works,
            person_film_works=person_film_works,
        )
