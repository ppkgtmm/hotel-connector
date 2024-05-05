from sqlalchemy import create_engine, text
from os import getenv


def setup_replication_user():
    connection_str = f"postgresql://{getenv('DB_USER')}:{getenv('DB_PASSWORD')}@{getenv('DB_ENDPOINT')}/{getenv('DB_NAME')}"
    engine = create_engine(connection_str)
    conn = engine.connect()
    with open("setup_user/queries.sql") as fp:
        queries = (
            fp.read()
            .replace("${DBZ_ROLE}", getenv("DBZ_ROLE"))
            .replace("${DBZ_PASSWORD}", getenv("DBZ_PASSWORD"))
            .replace("${DB_NAME}", getenv("DB_NAME"))
        )
    conn.execute(text(queries))
    conn.commit()
    conn.close()
    engine.dispose()
