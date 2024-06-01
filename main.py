from os import getenv
from sqlalchemy import create_engine, text
from google.cloud.sql.connector import Connector, IPTypes
from google.pubsub_v1 import PublisherClient, SubscriberClient, BigQueryConfig
from google.cloud.bigquery import Client, Table


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


def prepare_bq_storage(table_names: list[str]):
    bq_client = Client()
    dataset_ref = getenv("GCP_PROJECT_ID") + "." + getenv("BQ_DATASET")
    schema = [{"name": "data", "type": "string"}]
    bq_client.create_dataset(dataset_ref, exists_ok=True)
    for table_name in table_names:
        table = Table(dataset_ref + "." + table_name, schema=schema)
        bq_client.delete_table(table, not_found_ok=True)
        bq_client.create_table(table)


def prepare_for_replication(request):
    pub_client = PublisherClient()
    sub_client = SubscriberClient()
    table_names = [str(item[0]) for item in prepare_source_database()]
    prepare_bq_storage(table_names)
    dataset_path = getenv("GCP_PROJECT_ID") + "." + getenv("BQ_DATASET")
    for table_name in table_names:
        topic_path = pub_client.topic_path(
            getenv("GCP_PROJECT_ID"), getenv("DB_NAME") + ".public." + table_name
        )
        subs_path = sub_client.subscription_path(getenv("GCP_PROJECT_ID"), table_name)
        bq_config = BigQueryConfig(table=dataset_path + "." + table_name)
        pub_client.create_topic(name=topic_path)
        sub_client.create_subscription(
            name=subs_path, topic=topic_path, bigquery_config=bq_config
        )

    return "success"
