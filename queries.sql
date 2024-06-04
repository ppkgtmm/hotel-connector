ALTER USER ${DB_USER} REPLICATION;

GRANT CREATE ON DATABASE ${DB_NAME} TO ${DB_USER};

SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename NOT IN ('user') ;
