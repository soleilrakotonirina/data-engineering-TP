#!/bin/bash
# Script pour attendre que MinIO soit prêt

set -e

host="$1"

echo "Attente de MinIO sur $host..."

# Attendre que MinIO soit accessible (30 tentatives max)
for i in {1..30}; do
  if curl -f "http://$host/minio/health/live" > /dev/null 2>&1; then
    echo "MinIO est prêt!"
    break
  fi
  echo "Tentative $i/30 - MinIO pas encore prêt, attente..."
  sleep 3
done

# Vérifier si MinIO est finalement accessible
if ! curl -f "http://$host/minio/health/live" > /dev/null 2>&1; then
  echo "ERREUR: MinIO n'est pas accessible après 90 secondes"
  exit 1
fi

echo "MinIO est opérationnel - démarrage de Dagster..."