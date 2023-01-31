from unittest import TestCase
import sqlite3
from datetime import datetime

import psycopg2
from psycopg2.extras import DictCursor

from config import PG_DSL, SQLITE_DB_PATH

from load_data import load_from_sqlite
from extractor import _dict_cursor_factory


class BaseLoadTestCase(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sqlite_conn = sqlite3.connect(SQLITE_DB_PATH)
        cls.pg_conn = psycopg2.connect(**PG_DSL, cursor_factory=DictCursor)
        cls.cur = cls.pg_conn.cursor()
        cls.sqlite_conn.row_factory = _dict_cursor_factory
        cls.s_cur = cls.sqlite_conn.cursor()
        cls._is_loaded = False

    @classmethod
    def tearDownClass(cls):

        cls.cur.execute("DELETE FROM person_film_work;")
        cls.cur.execute("DELETE FROM genre_film_work;")
        cls.cur.execute("DELETE FROM person;")
        cls.cur.execute("DELETE FROM film_work;")
        cls.cur.execute("DELETE FROM genre;")
        cls.pg_conn.commit()
        cls.pg_conn.close()
        cls.sqlite_conn.close()

    def _load_data(self):
        if not self._is_loaded:
            load_from_sqlite(self.s_cur, self.cur)
            self._is_loaded = True

    def _get_counts_from_pg(self) -> dict:
        self.cur.execute("SELECT COUNT(*) FROM film_work;")
        filmwork_count = self.cur.fetchone()[0]
        self.cur.execute("SELECT COUNT(*) FROM genre;")
        genre_count = self.cur.fetchone()[0]
        self.cur.execute("SELECT COUNT(*) FROM person;")
        person_count = self.cur.fetchone()[0]
        self.cur.execute("SELECT COUNT(*) FROM genre_film_work;")
        genre_filmwork_count = self.cur.fetchone()[0]
        self.cur.execute("SELECT COUNT(*) FROM person_film_work;")
        person_filmwork_count = self.cur.fetchone()[0]
        return {
            "filmwork": filmwork_count,
            "genre": genre_count,
            "person": person_count,
            "genre_filmwork": genre_filmwork_count,
            "person_filmwork": person_filmwork_count,
        }

    def _get_counts_from_sqlite(self) -> dict:

        self.s_cur.execute("SELECT COUNT(*) FROM film_work;")
        filmwork_count = self.s_cur.fetchone()["COUNT(*)"]
        self.s_cur.execute("SELECT COUNT(*) FROM genre;")
        genre_count = self.s_cur.fetchone()["COUNT(*)"]
        self.s_cur.execute("SELECT COUNT(*) FROM person;")
        person_count = self.s_cur.fetchone()["COUNT(*)"]
        self.s_cur.execute("SELECT COUNT(*) FROM genre_film_work;")
        genre_filmwork_count = self.s_cur.fetchone()["COUNT(*)"]
        self.s_cur.execute("SELECT COUNT(*) FROM person_film_work;")
        person_filmwork_count = self.s_cur.fetchone()["COUNT(*)"]
        return {
            "filmwork": filmwork_count,
            "genre": genre_count,
            "person": person_count,
            "genre_filmwork": genre_filmwork_count,
            "person_filmwork": person_filmwork_count,
        }


class TestDataLoading(BaseLoadTestCase):
    def test_filmwork_data_loading(self):
        self._load_data()

        self.cur.execute("SELECT * FROM film_work LIMIT 1;")
        res = self.cur.fetchall()
        self.assertTrue(res)

    def test_genre_data_loading(self):
        self._load_data()

        self.cur.execute("SELECT * FROM genre LIMIT 1;")
        res = self.cur.fetchall()
        self.assertTrue(res)

    def test_person_data_loading(self):
        self._load_data()

        self.cur.execute("SELECT * FROM person LIMIT 1;")
        res = self.cur.fetchall()
        self.assertTrue(res)

    def test_genre_filmwork_data_loading(self):
        self._load_data()

        self.cur.execute("SELECT * FROM genre_film_work LIMIT 1;")
        res = self.cur.fetchall()
        self.assertTrue(res)

    def test_person_filmwork_data_loading(self):
        self._load_data()

        self.cur.execute("SELECT * FROM person_film_work LIMIT 1;")
        res = self.cur.fetchall()
        self.assertTrue(res)


class TestIdempotency(BaseLoadTestCase):
    def _load_data_twice_and_get_counts(self):
        if not self._is_loaded:
            load_from_sqlite(self.s_cur, self.cur)
            self.counts1 = self._get_counts_from_pg()
            load_from_sqlite(self.s_cur, self.cur)
            self.counts2 = self._get_counts_from_pg()
            self._is_loaded = True

    def test_filmwork_idempotency(self):
        self._load_data_twice_and_get_counts()

        self.assertEqual(self.counts1["filmwork"], self.counts2["filmwork"])

    def test_genre_idempotency(self):
        self._load_data_twice_and_get_counts()

        self.assertEqual(self.counts1["genre"], self.counts2["genre"])

    def test_person_idempotency(self):
        self._load_data_twice_and_get_counts()

        self.assertEqual(self.counts1["person"], self.counts2["person"])

    def test_genre_filmwork_idempotency(self):
        self._load_data_twice_and_get_counts()

        self.assertEqual(self.counts1["genre_filmwork"], self.counts2["genre_filmwork"])

    def test_person_filmwork_idempotency(self):
        self._load_data_twice_and_get_counts()

        self.assertEqual(
            self.counts1["person_filmwork"], self.counts2["person_filmwork"]
        )


class TestLoadedDataCount(BaseLoadTestCase):
    def _load_data_and_get_counts(self):
        if not self._is_loaded:
            load_from_sqlite(self.s_cur, self.cur)
            self.pg_counts = self._get_counts_from_pg()
            self.sqlite_counts = self._get_counts_from_sqlite()
            self._is_loaded = True

    def test_filmwork_count_in_pg_equals_to_sqlite(self):
        self._load_data_and_get_counts()
        self.assertEqual(self.pg_counts["filmwork"], self.sqlite_counts["filmwork"])

    def test_genre_count_in_pg_equals_to_sqlite(self):
        self._load_data_and_get_counts()
        self.assertEqual(self.pg_counts["genre"], self.sqlite_counts["genre"])

    def test_person_count_in_pg_equals_to_sqlite(self):
        self._load_data_and_get_counts()
        self.assertEqual(self.pg_counts["person"], self.sqlite_counts["person"])

    def test_genre_filmwork_count_in_pg_equals_to_sqlite(self):
        self._load_data_and_get_counts()
        self.assertEqual(
            self.pg_counts["genre_filmwork"], self.sqlite_counts["genre_filmwork"]
        )

    def test_person_filmwork_count_in_pg_equals_to_sqlite(self):
        self._load_data_and_get_counts()
        self.assertEqual(
            self.pg_counts["person_filmwork"], self.sqlite_counts["person_filmwork"]
        )


# class TestLoadedDataMatches(BaseLoadTestCase):
#     def _load_data_and_get_data(self):
#         if not self._is_loaded:
#             load_from_sqlite(self.sqlite_conn, self.pg_conn)
#             self.pg_data = self._get_data_from_pg()
#             self.sqlite_data = self._get_data_from_sqlite()
#             self._is_loaded = True

#     def _get_data_from_pg(self) -> dict:
#         self.cur.execute("SELECT * FROM film_work ORDER BY id;")
#         filmwork_data = [PGFilmWork(**row) for row in self.cur.fetchall()]
#         self.cur.execute("SELECT * FROM genre ORDER BY id;")
#         genre_data = [PGGenre(**row) for row in self.cur.fetchall()]
#         self.cur.execute("SELECT * FROM person ORDER BY id;")
#         person_data = [PGPerson(**row) for row in self.cur.fetchall()]
#         self.cur.execute("SELECT * FROM genre_film_work ORDER BY id;")
#         genre_filmwork_data = [PGGenreFilmwork(**row) for row in self.cur.fetchall()]
#         self.cur.execute("SELECT * FROM person_film_work ORDER BY id;")
#         person_filmwork_data = [PGPersonFilmWork(**row) for row in self.cur.fetchall()]
#         return {
#             "movies": PGMovies(
#                 filmworks=filmwork_data,
#                 genres=genre_data,
#                 persons=person_data,
#             ),
#             "rel_movies": RelationalPGMovies(
#                 genre_film_works=genre_filmwork_data,
#                 person_film_works=person_filmwork_data,
#             ),
#         }

#     def _get_data_from_sqlite(self) -> dict:
#         s_movies = SQLiteExtractor(self.sqlite_conn).extract()
#         s_rel_movies = RelationalSQLiteExtractor(self.sqlite_conn).extract()
#         return {
#             "movies": s_movies,
#             "rel_movies": s_rel_movies,
#         }

#     def test_filmwork_data_in_pg_equals_to_sqlite(self):
#         self._load_data_and_get_data()

#         for p_row, s_row in zip(
#             self.pg_data["movies"].filmworks, self.sqlite_data["movies"].film_works
#         ):
#             self.assertEqual(s_row.id, p_row.id)
#             self.assertEqual(s_row.title, p_row.title)
#             self.assertEqual(s_row.description, p_row.description)
#             self.assertEqual(s_row.rating, p_row.rating)
#             self.assertEqual(s_row.type, p_row.type)
#             self.assertEqual(datetime.fromisoformat(s_row.created_at), p_row.created)
#             self.assertEqual(datetime.fromisoformat(s_row.updated_at), p_row.modified)
#             self.assertEqual(s_row.creation_date, p_row.creation_date)

#     def test_genre_data_in_pg_equals_to_sqlite(self):
#         self._load_data_and_get_data()

#         for p_row, s_row in zip(
#             self.pg_data["movies"].genres, self.sqlite_data["movies"].genres
#         ):
#             self.assertEqual(s_row.id, p_row.id)
#             self.assertEqual(s_row.name, p_row.name)
#             self.assertEqual(s_row.description, p_row.description)
#             self.assertEqual(datetime.fromisoformat(s_row.created_at), p_row.created)
#             self.assertEqual(datetime.fromisoformat(s_row.updated_at), p_row.modified)

#     def test_person_data_in_pg_equals_to_sqlite(self):
#         self._load_data_and_get_data()

#         for p_row, s_row in zip(
#             self.pg_data["movies"].persons, self.sqlite_data["movies"].persons
#         ):
#             self.assertEqual(s_row.id, p_row.id)
#             self.assertEqual(s_row.full_name, p_row.full_name)
#             self.assertEqual(datetime.fromisoformat(s_row.created_at), p_row.created)
#             self.assertEqual(datetime.fromisoformat(s_row.updated_at), p_row.modified)

#     def test_genre_filmwork_data_in_pg_equals_to_sqlite(self):
#         self._load_data_and_get_data()

#         for p_row, s_row in zip(
#             self.pg_data["rel_movies"].genre_film_works,
#             self.sqlite_data["rel_movies"].genre_film_works,
#         ):
#             self.assertEqual(s_row.id, p_row.id)
#             self.assertEqual(s_row.film_work_id, p_row.film_work_id)
#             self.assertEqual(s_row.genre_id, p_row.genre_id)
#             self.assertEqual(datetime.fromisoformat(s_row.created_at), p_row.created)

#     def test_person_filmwork_data_in_pg_equals_to_sqlite(self):
#         self._load_data_and_get_data()

#         for p_row, s_row in zip(
#             self.pg_data["rel_movies"].person_film_works,
#             self.sqlite_data["rel_movies"].person_film_works,
#         ):
#             self.assertEqual(s_row.id, p_row.id)
#             self.assertEqual(s_row.film_work_id, p_row.film_work_id)
#             self.assertEqual(s_row.person_id, p_row.person_id)
#             self.assertEqual(s_row.role, p_row.role)
#             self.assertEqual(datetime.fromisoformat(s_row.created_at), p_row.created)
