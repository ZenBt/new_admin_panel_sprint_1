import logging
import os
from logging import config

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

log_config = {
    "version":1,
    "root":
    {
        "handlers" : ["console"],
        "level": "DEBUG"
    },
    "handlers":
    {
        "console":{
            "formatter": "std_out",
            "class": "logging.StreamHandler",
            "level": "DEBUG"
        }
    },
    "formatters":
    {
        "std_out": {
            "format": "%(asctime)s : %(levelname)s : %(module)s : %(funcName)s : %(lineno)d : (Process Details : (%(process)d, %(processName)s), Thread Details : (%(thread)d, %(threadName)s))\nLog : %(message)s",
            "datefmt":"%d-%m-%Y %I:%M:%S"
        }
    },
}

config.dictConfig(log_config)
