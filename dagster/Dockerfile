FROM python:3.12-slim

# Installer les dépendances
RUN pip install dagster dagster-webserver pandas openpyxl

# Définir l'environnement Dagster
ENV DAGSTER_HOME=/opt/dagster/dagster_home
RUN mkdir -p /opt/dagster/dagster_home

# Copier le code dans l'image Docker
WORKDIR /opt/dagster/app
COPY ./dagster /opt/dagster/app
