ALTER USER ${DB_USER} REPLICATION;

GRANT CREATE ON DATABASE ${DB_NAME} TO ${DB_USER};

select tablename from pg_tables where schemaname = 'public';
