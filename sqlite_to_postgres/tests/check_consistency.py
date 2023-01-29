from unittest import TestCase
import sqlite3

import psycopg2
from psycopg2.extras import DictCursor

from config import PG_DSL, SQLITE_DB_PATH
from load_data import load_from_sqlite


class BaseLoadTestCase(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sqlite_conn = sqlite3.connect(SQLITE_DB_PATH)
        cls.pg_conn = psycopg2.connect(**PG_DSL, cursor_factory=DictCursor)
        cls.cur = cls.pg_conn.cursor()
        cls.sqlite_conn.row_factory = sqlite3.Row
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
            load_from_sqlite(self.sqlite_conn, self.pg_conn)
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
        filmwork_count = self.s_cur.fetchone()[0]
        self.s_cur.execute("SELECT COUNT(*) FROM genre;")
        genre_count = self.s_cur.fetchone()[0]
        self.s_cur.execute("SELECT COUNT(*) FROM person;")
        person_count = self.s_cur.fetchone()[0]
        self.s_cur.execute("SELECT COUNT(*) FROM genre_film_work;")
        genre_filmwork_count = self.s_cur.fetchone()[0]
        self.s_cur.execute("SELECT COUNT(*) FROM person_film_work;")
        person_filmwork_count = self.s_cur.fetchone()[0]
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
            load_from_sqlite(self.sqlite_conn, self.pg_conn)
            self.counts1 = self._get_counts_from_pg()
            load_from_sqlite(self.sqlite_conn, self.pg_conn)
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

        self.assertEqual(self.counts1["person_filmwork"], self.counts2["person_filmwork"])


class TestLoadedDataCount(BaseLoadTestCase):

    def _load_data_and_get_counts(self):
        if not self._is_loaded:
            load_from_sqlite(self.sqlite_conn, self.pg_conn)
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
        self.assertEqual(self.pg_counts["genre_filmwork"], self.sqlite_counts["genre_filmwork"])

    def test_person_filmwork_count_in_pg_equals_to_sqlite(self):
        self._load_data_and_get_counts()
        self.assertEqual(self.pg_counts["person_filmwork"], self.sqlite_counts["person_filmwork"])


class TestLoadedDataMatches(BaseLoadTestCase):

    def _load_data_and_get_data(self):
        if not self._is_loaded:
            load_from_sqlite(self.sqlite_conn, self.pg_conn)
            self.pg_data = self._get_data_from_pg()
            self.sqlite_data = self._get_data_from_sqlite()
            self._is_loaded = True
    
    def _get_data_from_pg(self) -> dict:
        self.cur.execute("SELECT * FROM film_work ORDER BY id;")
        filmwork_data = self.cur.fetchall()
        self.cur.execute("SELECT * FROM genre ORDER BY id;")
        genre_data = self.cur.fetchall()
        self.cur.execute("SELECT * FROM person ORDER BY id;")
        person_data = self.cur.fetchall()
        self.cur.execute("SELECT * FROM genre_film_work; ORDER BY id")
        genre_filmwork_data = self.cur.fetchall()
        self.cur.execute("SELECT * FROM person_film_work ORDER BY id;")
        person_filmwork_data = self.cur.fetchall()
        return {
            "filmwork": filmwork_data,
            "genre": genre_data,
            "person": person_data,
            "genre_filmwork": genre_filmwork_data,
            "person_filmwork": person_filmwork_data,
        }
    
    def _get_data_from_sqlite(self) -> dict:
        self.s_cur.execute("SELECT * FROM film_work ORDER BY id;")
        filmwork_data = self.s_cur.fetchall()
        self.s_cur.execute("SELECT * FROM genre ORDER BY id;")
        genre_data = self.s_cur.fetchall()
        self.s_cur.execute("SELECT * FROM person ORDER BY id;")
        person_data = self.s_cur.fetchall()
        self.s_cur.execute("SELECT * FROM genre_film_work ORDER BY id;")
        genre_filmwork_data = self.s_cur.fetchall()
        self.s_cur.execute("SELECT * FROM person_film_work ORDER BY id;")
        person_filmwork_data = self.s_cur.fetchall()
        return {
            "filmwork": filmwork_data,
            "genre": genre_data,
            "person": person_data,
            "genre_filmwork": genre_filmwork_data,
            "person_filmwork": person_filmwork_data,
        }
    
    def test_filmwork_data_in_pg_equals_to_sqlite(self):
        self._load_data_and_get_data()
        self.assertEqual(self.pg_data["filmwork"], self.sqlite_data["filmwork"])
    
    def test_genre_data_in_pg_equals_to_sqlite(self):
        self._load_data_and_get_data()
        self.assertEqual(self.pg_data["genre"], self.sqlite_data["genre"])
    
    def test_person_data_in_pg_equals_to_sqlite(self):
        self._load_data_and_get_data()
        self.assertEqual(self.pg_data["person"], self.sqlite_data["person"])
    
    def test_genre_filmwork_data_in_pg_equals_to_sqlite(self):
        self._load_data_and_get_data()
        self.assertEqual(self.pg_data["genre_filmwork"], self.sqlite_data["genre_filmwork"])
    
    def test_person_filmwork_data_in_pg_equals_to_sqlite(self):
        self._load_data_and_get_data()
        self.assertEqual(self.pg_data["person_filmwork"], self.sqlite_data["person_filmwork"])
