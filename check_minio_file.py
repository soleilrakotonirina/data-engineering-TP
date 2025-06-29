import boto3

client = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin",
)

response = client.list_objects_v2(Bucket="datatest")

print("📁 Fichiers dans le bucket 'datatest' :")
for obj in response.get("Contents", []):
    print("-", obj["Key"])
