from os import getenv
from sqlalchemy import create_engine, text
from google.cloud.sql.connector import Connector, IPTypes
from google.cloud.pubsub import PublisherClient


def get_db_connection():
    connector = Connector(IPTypes.PUBLIC)
    return connector.connect(
        getenv("CONNECTION"),
        "pg8000",
        user=getenv("ROOT_USER"),
        password=getenv("ROOT_PASSWORD"),
        db=getenv("DB_NAME"),
    )


def prepare_for_replication():
    with open("queries.sql", "r") as fp:
        template = fp.read()
    template = template.replace("${DB_USER}", getenv("DB_USER")).replace(
        "${DB_NAME}", getenv("DB_NAME")
    )
    engine = create_engine("postgresql+pg8000://", creator=get_db_connection)
    with engine.connect() as conn:
        table_names = conn.execute(text(template)).fetchall()
        conn.commit()
    engine.dispose()
    return table_names


def handler(request):
    client = PublisherClient()
    for item in prepare_for_replication():
        topic_path = client.topic_path(
            getenv("GCP_PROJECT_ID"), getenv("DB_NAME") + ".public." + item[0]
        )
        client.create_topic(request={"name": topic_path})
    return "success"
