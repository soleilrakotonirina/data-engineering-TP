#!/bin/bash
# Script de débogage et démarrage de Dagster

set -e

echo "=== Débogage Dagster ==="

# Vérifier les variables d'environnement
echo "Variables d'environnement:"
echo "DAGSTER_HOME: ${DAGSTER_HOME}"
echo "MINIO_ENDPOINT: ${MINIO_ENDPOINT}"
echo "MINIO_ACCESS_KEY: ${MINIO_ACCESS_KEY}"
echo "PYTHONPATH: ${PYTHONPATH}"

# Vérifier la structure des répertoires
echo "Structure des répertoires:"
ls -la /opt/dagster/

# Vérifier les fichiers Python
echo "Fichiers pipelines:"
ls -la /opt/dagster/pipelines/ || echo "Répertoire pipelines non trouvé"

# Vérifier la configuration Dagster
echo "Configuration Dagster:"
ls -la /opt/dagster/dagster_home/ || echo "Répertoire dagster_home non trouvé"

# Tester la connectivité MinIO
echo "Test connectivité MinIO:"
curl -f "http://${MINIO_ENDPOINT}/minio/health/live" && echo "MinIO OK" || echo "MinIO KO"

# Vérifier que le module Python peut être importé
echo "Test import du module:"
python -c "import pipelines.main_pipeline; print('Import OK')" || echo "Erreur d'import"

# Démarrer Dagster
echo "Démarrage de Dagster webserver..."
exec dagster-webserver -h 0.0.0.0 -p 3000