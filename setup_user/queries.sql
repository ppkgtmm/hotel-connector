CREATE ROLE ${DBZ_ROLE} LOGIN PASSOWRD ${DBZ_PASSWORD};

GRANT rds_replication TO ${DBZ_ROLE};

GRANT SELECT ON ALL TABLES IN SCHEMA public to ${DBZ_ROLE};

GRANT CREATE ON DATABASE ${DB_NAME} TO ${DBZ_ROLE};

GRANT ${DBZ_ROLE} TO rds_superuser;

REASSIGN OWNED BY rds_superuser TO ${DBZ_ROLE}
