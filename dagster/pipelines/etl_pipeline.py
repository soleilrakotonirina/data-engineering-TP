# dagster/pipelines/etl_pipeline.py

from dagster import job, op
import pandas as pd
import duckdb
import boto3
import io

@op
def extract_transform():
    # Connexion à MinIO
    s3 = boto3.client(
        's3',
        endpoint_url='http://minio:9000',
        aws_access_key_id='minioadmin',
        aws_secret_access_key='minioadmin'
    )

    # Télécharger ton fichier depuis le bucket MinIO
    obj = s3.get_object(Bucket='datatest', Key='2.La-mise-en-forme-conditionnelle-et-la-liste-deroulante.xlsx')
    df = pd.read_excel(io.BytesIO(obj['Body'].read()))

    print("Colonnes dans le fichier :", df.columns.tolist())

    # Nettoyage avec DuckDB — tu peux adapter la requête selon le contenu réel
    con = duckdb.connect()
    con.register('df', df)
    
    # EXEMPLE : filtre les lignes où une colonne 'Prix' > 0 (adapte le nom réel)
    try:
        df_clean = con.execute("SELECT * FROM df WHERE Prix > 0").fetchdf()
    except Exception as e:
        print("Erreur DuckDB :", e)
        df_clean = df  # si pas de colonne 'Prix', on renvoie tout

    # Sauvegarder le résultat en Parquet dans le bucket datatest, dossier silver
    buffer = io.BytesIO()
    df_clean.to_parquet(buffer, index=False)
    buffer.seek(0)
    s3.put_object(Bucket='datatest', Key='silver/format_conditionnelle_clean.parquet', Body=buffer)

    return "Traitement terminé"

@job
def etl_job():
    extract_transform()
