import requests
import json
from sqlalchemy import create_engine, text


def get_configuration(DB_HOST: str, DBZ_USER: str, DBZ_PASSWORD: str, DB_NAME: str):
    with open("connector-config.json", "r") as fp:
        config = fp.read()
    config = (
        config.replace("${DB_HOST}", DB_HOST.split(":")[0])
        .replace("${DBZ_USER}", DBZ_USER)
        .replace("${DBZ_PASSWORD}", DBZ_PASSWORD)
        .replace("${DB_NAME}", DB_NAME)
    )
    return config


def register_source_database(config: str, KAFKA_CONNECT_SERVER:str):
    endpoint = f"{KAFKA_CONNECT_SERVER}/connectors/"
    headers = {"Accept": "application/json", "Content-Type": "application/json"}
    response = requests.post(endpoint, headers=headers, json=json.loads(config))
    return "success" if response.status_code == 201 else "failed"


def get_query_template(DBZ_USER: str, DBZ_PASSWORD: str, DB_NAME: str):
    with open("queries.sql", "r") as fp:
        template = fp.read()
    template = (
        template.replace("${DBZ_USER}", DBZ_USER)
        .replace("${DBZ_PASSWORD}", DBZ_PASSWORD)
        .replace("${DB_NAME}", DB_NAME)
    )
    return template


def lambda_handler(event, context):
    engine = create_engine(f'postgresql://{event['DB_USER']}:{event['DB_PASSWORD']}@{event['DB_HOST']}/{event['DB_NAME']}')
    conn = engine.connect()
    template = get_query_template(event['DBZ_USER'], event['DBZ_PASSWORD'], event['DB_NAME'])
    conn.execute(text(template))
    conn.close()
    engine.dispose()
    config = get_configuration(event['DB_HOST'], event['DBZ_USER'], event['DBZ_PASSWORD'], event['DB_NAME'])
    return register_source_database(config, event['KAFKA_CONNECT_SERVER'])
