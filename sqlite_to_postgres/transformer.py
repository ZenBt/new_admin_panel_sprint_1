from dto import (
    SQLiteMovies,
    PGMovies,
    SQLitePerson,
    SQLiteFilmWork,
    SQLiteGenre,
    SQLiteGenreFilmwork,
    SQLitePersonFilmWork,
    PGFilmWork,
    PGPerson,
    PGGenre,
    PGGenreFilmwork,
    PGPersonFilmWork,
)


class SQLiteToPGTransformer:
    def __init__(self):
        self._sqlite_data: SQLiteMovies | None = None

    @property
    def sqlite_data(self) -> SQLiteMovies:
        return self._sqlite_data

    @sqlite_data.setter
    def sqlite_data(self, value: SQLiteMovies) -> None:
        self._sqlite_data = value

    def transform(self) -> PGMovies:
        """Transform SQLite data to Postgres data

        :return: Postgres data
        :raises AssertionError: if SQLite data is not set
        """
        assert self.sqlite_data is not None, "SQLite data is not set"
        
        filmworks = self._transform_filmworks()
        genres = self._transform_genres()
        persons = self._transform_persons()
        genre_filmworks = self._transform_genre_filmworks()
        person_filmworks = self._transform_person_filmworks()
        
        return self._make_pg_data(
            filmworks=filmworks,
            genres=genres,
            persons=persons,
            genre_filmworks=genre_filmworks,
            person_filmworks=person_filmworks,
        )


    def _transform_persons(self) -> list[PGPerson]:

        return []

    def _transform_filmworks(self) -> list[PGFilmWork]:
        return []

    def _transform_genres(self) -> list[PGGenre]:

        return []

    def _transform_genre_filmworks(self) -> list[PGGenreFilmwork]:

        return []

    def _transform_person_filmworks(self) -> list[PGPersonFilmWork]:

        return []

    def _make_pg_data(
        self,
        filmworks: list[PGFilmWork],
        genres: list[PGFilmWork],
        persons: list[PGPerson],
        genre_filmworks: list[PGGenreFilmwork],
        person_filmworks: list[PGPersonFilmWork],
    ) -> PGMovies:
        return PGMovies(
            filmworks=filmworks,
            genres=genres,
            persons=persons,
            genre_filmworks=genre_filmworks,
            person_filmworks=person_filmworks,
        )
