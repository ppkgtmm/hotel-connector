cat > query.sql << EOF
CREATE USER $DBZ_USER PASSWORD '$DBZ_PASSWORD';

GRANT LOGIN TO $DBZ_USER;

GRANT rds_superuser TO $DBZ_USER;

GRANT SELECT ON ALL TABLES IN SCHEMA public TO $DBZ_USER;

GRANT CREATE ON DATABASE $DB_NAME TO $DBZ_USER;

SELECT 'ALTER TABLE "' || table_name || '" OWNER TO $DBZ_USER;' from information_schema.tables where table_schema = 'public' \gexec
EOF
