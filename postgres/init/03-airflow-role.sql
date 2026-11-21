SELECT format('CREATE ROLE %I LOGIN PASSWORD %L', :'airflow_user', :'airflow_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'airflow_user')
\gexec
SELECT format('ALTER ROLE %I WITH LOGIN PASSWORD %L', :'airflow_user', :'airflow_password')
\gexec

GRANT CONNECT ON DATABASE :"db_name" TO :"airflow_user";
GRANT USAGE, CREATE ON SCHEMA public TO :"airflow_user";
