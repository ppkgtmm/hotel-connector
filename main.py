from setup_user.main import setup_replication_user
from register_source.main import register_source_database


def entrypoint():
    setup_replication_user()
    register_source_database()
