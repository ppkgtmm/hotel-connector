from os import getenv
from sqlalchemy import create_engine, text
from google.cloud.sql.connector import Connector, IPTypes
from google.cloud.pubsublite.types import (
    CloudRegion,
    CloudZone,
    TopicPath,
    SubscriptionPath,
    BacklogLocation,
)
from google.cloud.pubsublite import AdminClient, Topic, Subscription
from google.protobuf.duration_pb2 import Duration


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


def get_topic_config(table_name: str, location: CloudZone):
    topic_path = TopicPath(
        getenv("GCP_PROJECT_ID"),
        location,
        getenv("DB_NAME") + ".public." + table_name,
    )
    return topic_path, Topic(
        name=str(topic_path),
        partition_config=Topic.PartitionConfig(
            count=1,
            capacity=Topic.PartitionConfig.Capacity(
                publish_mib_per_sec=4,
                subscribe_mib_per_sec=4,
            ),
        ),
        retention_config=Topic.RetentionConfig(
            per_partition_bytes=30 * 1024 * 1024 * 1024,
            period=Duration(seconds=60 * 60 * 24 * 1),
        ),
    )


def get_subscription_config(
    table_name: str, location: CloudZone, topic_path: TopicPath
):
    subscription_path = SubscriptionPath(getenv("GCP_PROJECT_ID"), location, table_name)
    return Subscription(
        name=str(subscription_path),
        topic=str(topic_path),
        delivery_config=Subscription.DeliveryConfig(
            delivery_requirement=Subscription.DeliveryConfig.DeliveryRequirement.DELIVER_AFTER_STORED,
        ),
    )


def prepare_pubsub_topics(table_names: list[str]):
    cloud_region = CloudRegion(getenv("GCP_REGION"))
    location = CloudZone(cloud_region, getenv("GCP_ZONE"))
    client = AdminClient(cloud_region)
    for table_name in table_names:
        topic_path, topic = get_topic_config(table_name, location)
        client.create_topic(topic)
        subscription = get_subscription_config(table_name, location, topic_path)
        client.create_subscription(subscription, target=BacklogLocation.BEGINNING)


def prepare_for_replication(request):
    table_names = [str(item[0]) for item in prepare_source_database()]
    prepare_pubsub_topics(table_names)
    return "success"
