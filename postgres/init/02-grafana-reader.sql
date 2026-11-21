SELECT format('CREATE ROLE %I LOGIN PASSWORD %L', :'grafana_user', :'grafana_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'grafana_user')
\gexec
SELECT format('ALTER ROLE %I WITH LOGIN PASSWORD %L', :'grafana_user', :'grafana_password')
\gexec

GRANT CONNECT ON DATABASE :"db_name" TO :"grafana_user";
GRANT USAGE ON SCHEMA public TO :"grafana_user";
GRANT SELECT ON ALL TABLES IN SCHEMA public TO :"grafana_user";
ALTER DEFAULT PRIVILEGES FOR ROLE :"app_user" IN SCHEMA public
    GRANT SELECT ON TABLES TO :"grafana_user";
