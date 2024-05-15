from os import getenv
from sqlalchemy import create_engine


def get_query_template():
    with open("replication.sql", "r") as fp:
        template = fp.read()
    template = (
        template.replace("${DBZ_USER}", getenv("DBZ_USER"))
        .replace("${DBZ_PASSWORD}", getenv("DBZ_PASSWORD"))
        .replace("${DB_NAME}", getenv("DB_NAME"))
    )
    return template


def prepare_for_replication():
    username, password = getenv("DB_USER"), getenv("DB_PASSWORD")
    host, database = getenv("DB_HOST"), getenv("DB_NAME")
    engine = create_engine(f"postgresql://{username}:{password}@{host}/{database}")
    conn = engine.connect()
    result = conn.execute(get_query_template())
    for item in result.fetchall():
        ownership_query = 'ALTER TABLE "' + item[0] + f'" OWNER TO {getenv("DBZ_USER")}'
        conn.execute(ownership_query)
    conn.close()
    engine.dispose()

def prepare_data_warehouse():
    username, password = getenv("DWH_USER"), getenv("DWH_PASSWORD")
    host, database = getenv("DWH_HOST"), getenv("DWH_NAME")
    engine = create_engine(f"redshift+psycopg2://{username}:{password}@{host}/{database}")
    conn = engine.connect()
    with open("warehouse.sql", "r") as fp:
        conn.execute(fp.read())
    conn.close()
    engine.dispose()

def handler(event, context):
    prepare_for_replication()
    prepare_data_warehouse()
    return "success"
