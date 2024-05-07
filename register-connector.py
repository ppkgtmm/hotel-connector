from os import getenv
import requests
import json


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


def register_source_database():
    config = get_configuration()
    endpoint = f"{getenv("KAFKA_CONNECT_SERVER")}/connectors/"
    headers = {"Accept": "application/json", "Content-Type": "application/json"}
    response = requests.post(endpoint, headers=headers, json=json.loads(config))
    assert response.status_code == 201
