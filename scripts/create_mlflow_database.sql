SELECT 'CREATE DATABASE mlflow OWNER automl'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'mlflow')
\gexec
