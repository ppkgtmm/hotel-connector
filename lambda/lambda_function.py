from os import getenv
import requests
import json
from sqlalchemy import create_engine, text


def get_configuration():
    with open("connector-config.json", "r") as fp:
        config = fp.read()
    config = (
        config.replace("${DB_HOST}", getenv("DB_HOST").split(":")[0])
        .replace("${DBZ_USER}", getenv("DBZ_USER"))
        .replace("${DBZ_PASSWORD}", getenv("DBZ_PASSWORD"))
        .replace("${DB_NAME}", getenv("DB_NAME"))
    )
    return config


def register_source_database(config: str):
    endpoint = f"{getenv("KAFKA_CONNECT_SERVER")}/connectors/"
    headers = {"Accept": "application/json", "Content-Type": "application/json"}
    response = requests.post(endpoint, headers=headers, json=json.loads(config))
    return "success" if response.status_code == 201 else "failed"


def get_query_template():
    with open("queries.sql", "r") as fp:
        template = fp.read()
    template = (
        template.replace("${DBZ_USER}", getenv("DBZ_USER"))
        .replace("${DBZ_PASSWORD}", getenv("DBZ_PASSWORD"))
        .replace("${DB_NAME}", getenv("DB_NAME"))
    )
    return template


def lambda_handler(event):
    engine = create_engine(f'postgresql://{getenv("DB_USER")}:{getenv("DB_PASSWORD")}@{getenv("DB_HOST")}/{getenv("DB_NAME")}')
    conn = engine.connect()
    template = get_query_template()
    conn.execute(text(template))
    conn.close()
    engine.dispose()
    config = get_configuration()
    return register_source_database(config)
