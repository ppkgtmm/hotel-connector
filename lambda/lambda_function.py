from os import getenv
from sqlalchemy import create_engine, text


def get_query_template():
    with open("queries.sql", "r") as fp:
        template = fp.read()
    template = (
        template.replace("${DBZ_USER}", getenv("DBZ_USER"))
        .replace("${DBZ_PASSWORD}", getenv("DBZ_PASSWORD"))
        .replace("${DB_NAME}", getenv("DB_NAME"))
    )
    return template


def lambda_handler(event, context):
    username, password = getenv("DB_USER"), getenv("DB_PASSWORD")
    host, database = getenv("DB_HOST"), getenv("DB_NAME")
    engine = create_engine(f"postgresql://{username}:{password}@{host}/{database}")
    conn = engine.connect()
    result = conn.execute(text(get_query_template()))
    conn.commit()
    for item in result.fetchall():
        ownership_query = 'ALTER TABLE "' + item[0] + f'" OWNER TO {getenv("DBZ_USER")}'
        conn.execute(text(ownership_query))
    conn.close()
    engine.dispose()
