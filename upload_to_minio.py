import boto3

client = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",  # ou "http://minio:9000" si exécuté dans un conteneur
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin"
)

with open("/home/sun/Documents/bareme_solde.xlsx", "rb") as f:
    client.upload_fileobj(f, "datatest", "bareme_solde.xlsx")

print("✅ Fichier mis à jour dans MinIO.")
