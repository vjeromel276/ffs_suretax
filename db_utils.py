import psycopg2
from psycopg2.extras import RealDictCursor

def get_db_conn():
    return psycopg2.connect(
        dbname="GLC",
        user="oss_server",
        password="3wU3uB28X?!r2?@ebrUg",
        host="pg01.comlink.net",
        port="5432",
        cursor_factory=RealDictCursor
    )
