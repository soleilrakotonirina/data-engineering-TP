import os
import pandas as pd
import duckdb
from minio import Minio
from minio.error import S3Error
from dagster import (
    asset,
    Config,
    AssetExecutionContext,
    Definitions,
    MaterializeResult,
    MetadataValue
)
from io import BytesIO
import tempfile

# Configuration MinIO
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'localhost:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minioadmin123')
BUCKET_NAME = 'datatest'

class MinIOConfig(Config):
    excel_file_name: str = "data.xlsx"

def get_minio_client():
    """Créer et retourner un client MinIO"""
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

def ensure_bucket_exists(client: Minio, bucket_name: str):
    """Créer le bucket s'il n'existe pas"""
    try:
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            print(f"Bucket '{bucket_name}' créé avec succès")
        else:
            print(f"Bucket '{bucket_name}' existe déjà")
    except S3Error as e:
        print(f"Erreur lors de la création du bucket: {e}")
        raise

@asset(
    name="bronze_data",
    description="Données brutes Excel depuis MinIO (niveau Bronze)"
)
def extract_excel_from_minio(context: AssetExecutionContext, config: MinIOConfig) -> pd.DataFrame:
    """
    Asset Bronze: Extraction du fichier Excel depuis MinIO
    """
    client = get_minio_client()
    
    try:
        # Vérifier si le bucket existe
        ensure_bucket_exists(client, BUCKET_NAME)
        
        # Télécharger le fichier Excel depuis MinIO
        context.log.info(f"Téléchargement de {config.excel_file_name} depuis MinIO...")
        
        response = client.get_object(BUCKET_NAME, config.excel_file_name)
        excel_data = response.read()
        
        # Lire le fichier Excel avec pandas
        df = pd.read_excel(BytesIO(excel_data))
        
        context.log.info(f"Fichier Excel lu avec succès. Shape: {df.shape}")
        context.log.info(f"Colonnes: {list(df.columns)}")
        
        return df
        
    except S3Error as e:
        context.log.error(f"Erreur MinIO: {e}")
        # Si le fichier n'existe pas, créer des données d'exemple
        context.log.info("Création de données d'exemple...")
        sample_data = {
            'id': [1, 2, 3, 4, 5],
            'nom': ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve'],
            'age': [25, 30, 35, 28, 32],
            'ville': ['Paris', 'Lyon', 'Marseille', 'Toulouse', 'Nice']
        }
        return pd.DataFrame(sample_data)
    except Exception as e:
        context.log.error(f"Erreur lors de la lecture du fichier Excel: {e}")
        raise
    finally:
        if 'response' in locals():
            response.close()
            response.release_conn()

@asset(
    name="silver_data",
    description="Données transformées au format Parquet (niveau Silver)",
    deps=[bronze_data]
)
def transform_to_parquet(context: AssetExecutionContext, bronze_data: pd.DataFrame) -> MaterializeResult:
    """
    Asset Silver: Transformation des données Bronze vers Silver avec DuckDB
    """
    try:
        # Utiliser DuckDB pour les transformations
        conn = duckdb.connect()
        
        # Enregistrer le DataFrame pandas dans DuckDB
        conn.register('bronze_data', bronze_data)
        
        # Effectuer des transformations (exemple: nettoyage et typage)
        context.log.info("Transformation des données avec DuckDB...")
        
        # Exemple de transformations SQL
        silver_query = """
        SELECT 
            *,
            CURRENT_TIMESTAMP as processed_at
        FROM bronze_data
        WHERE bronze_data IS NOT NULL
        """
        
        # Exécuter la requête et récupérer le résultat
        silver_df = conn.execute(silver_query).fetchdf()
        
        context.log.info(f"Données transformées. Shape: {silver_df.shape}")
        
        # Sauvegarder en format Parquet localement
        silver_path = "/opt/dagster/data/silver_data.parquet"
        os.makedirs(os.path.dirname(silver_path), exist_ok=True)
        silver_df.to_parquet(silver_path, index=False)
        
        context.log.info(f"Fichier Parquet sauvegardé: {silver_path}")
        
        # Optionnel: Uploader vers MinIO
        client = get_minio_client()
        try:
            client.fput_object(
                BUCKET_NAME, 
                "silver/silver_data.parquet", 
                silver_path
            )
            context.log.info("Fichier Parquet uploadé vers MinIO (silver/)")
        except S3Error as e:
            context.log.warning(f"Erreur upload MinIO: {e}")
        
        return MaterializeResult(
            metadata={
                "num_rows": len(silver_df),
                "num_columns": len(silver_df.columns),
                "file_path": MetadataValue.path(silver_path),
                "file_size_bytes": os.path.getsize(silver_path)
            }
        )
        
    except Exception as e:
        context.log.error(f"Erreur transformation: {e}")
        raise
    finally:
        conn.close()

@asset(
    name="data_quality_check",
    description="Vérification de la qualité des données Silver",
    deps=[silver_data]
)
def validate_silver_data(context: AssetExecutionContext) -> MaterializeResult:
    """
    Asset de validation: Vérifier la qualité des données Silver
    """
    try:
        # Lire le fichier Parquet
        silver_path = "/opt/dagster/data/silver_data.parquet"
        df = pd.read_parquet(silver_path)
        
        # Contrôles qualité
        null_counts = df.isnull().sum()
        duplicate_rows = df.duplicated().sum()
        
        context.log.info(f"Contrôle qualité terminé:")
        context.log.info(f"- Lignes totales: {len(df)}")
        context.log.info(f"- Lignes dupliquées: {duplicate_rows}")
        context.log.info(f"- Valeurs nulles par colonne: {null_counts.to_dict()}")
        
        return MaterializeResult(
            metadata={
                "total_rows": len(df),
                "duplicate_rows": duplicate_rows,
                "null_values": null_counts.to_dict(),
                "quality_score": MetadataValue.float(max(0, 100 - (duplicate_rows * 10) - (null_counts.sum() * 5)))
            }
        )
        
    except Exception as e:
        context.log.error(f"Erreur validation: {e}")
        raise

# Définition des assets pour Dagster
defs = Definitions(
    assets=[bronze_data, silver_data, data_quality_check],
)