FROM python:3.12-slim

# Installer les dépendances
RUN pip install dagster dagster-webserver pandas openpyxl xlrd duckdb boto3 s3fs sqlalchemy psycopg2-binary pytest pytest-cov

# Définir l'environnement Dagster
ENV DAGSTER_HOME=/opt/dagster/dagster_home
RUN mkdir -p /opt/dagster/dagster_home

# Définir le dossier de travail
WORKDIR /opt/dagster/app

# Copier le contenu du build context (c'est-à-dire le dossier 'dagster/') dans le container
COPY . /opt/dagster/app
