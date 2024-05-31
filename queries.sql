ALTER USER ${DB_USER} REPLICATION;

GRANT CREATE ON DATABASE ${DB_NAME} TO ${DB_USER};

SELECT table_name from information_schema.tables where table_schema = 'public';
