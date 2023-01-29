import os

from dotenv import load_dotenv

load_dotenv()

PG_DSL = {
    "dbname": os.getenv("PG_DBNAME"),
    "user": os.getenv("PG_USER"),
    "password": os.getenv("PG_PASSWORD"),
    "host": os.getenv("PG_HOST"),
    "port": os.getenv("PG_PORT"),
}

SQLITE_DB_PATH = os.getenv("SQLITE_DB_PATH")

CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", 1000))