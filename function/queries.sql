CREATE USER ${DBZ_USER} PASSWORD '${DBZ_PASSWORD}';

GRANT rds_superuser, rds_replication TO ${DBZ_USER};

GRANT SELECT ON ALL TABLES IN SCHEMA public TO ${DBZ_USER};

GRANT CREATE ON DATABASE ${DB_NAME} TO ${DBZ_USER};

SELECT table_name from information_schema.tables where table_schema = 'public';
