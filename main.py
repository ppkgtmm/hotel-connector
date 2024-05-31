from os import getenv
from sqlalchemy import create_engine, text
from google.cloud.sql.connector import Connector, IPTypes
from google.pubsub_v1 import PublisherClient, SubscriberClient


def get_db_connection():
    connector = Connector(IPTypes.PUBLIC)
    return connector.connect(
        getenv("CONNECTION"),
        "pg8000",
        user=getenv("ROOT_USER"),
        password=getenv("ROOT_PASSWORD"),
        db=getenv("DB_NAME"),
    )


def prepare_source_database():
    with open("queries.sql", "r") as fp:
        template = fp.read()
    template = template.replace("${DB_USER}", getenv("DB_USER")).replace(
        "${DB_NAME}", getenv("DB_NAME")
    )
    engine = create_engine("postgresql+pg8000://", creator=get_db_connection)
    with engine.connect() as conn:
        table_names = conn.execute(text(template))
        conn.commit()
    engine.dispose()
    return table_names.fetchall()


def prepare_for_replication(request):
    pub_client = PublisherClient()
    sub_client = SubscriberClient()
    for item in prepare_source_database():
        topic_path = pub_client.topic_path(
            getenv("GCP_PROJECT_ID"), getenv("DB_NAME") + ".public." + item[0]
        )
        pub_client.create_topic(name=topic_path)
        subs_path = sub_client.subscription_path(getenv("GCP_PROJECT_ID"), item[0])
        sub_client.create_subscription(name=subs_path, topic=topic_path)

    return "success"
