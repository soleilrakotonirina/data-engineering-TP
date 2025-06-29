from dagster import Definitions, job, op, get_dagster_logger, In, Out
import pandas as pd
import boto3
import duckdb
from io import BytesIO
from sqlalchemy import create_engine, text

# -----------------------------
# Étape 1 & 2 : MinIO → DuckDB
# -----------------------------

@op
def load_from_minio():
    logger = get_dagster_logger()
    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
    )
    try:
        obj = s3.get_object(Bucket="datatest", Key="bareme_solde.xlsx")
        df = pd.read_excel(BytesIO(obj["Body"].read()))
        logger.info("✅ Fichier chargé depuis MinIO.\nAperçu :\n" + df.head().to_string())
        return df
    except Exception as e:
        logger.error(f"❌ Erreur lors du chargement depuis MinIO : {e}")
        raise

@op
def count_rows_minio(df: pd.DataFrame) -> int:
    count = len(df)
    get_dagster_logger().info(f"🔢 MinIO contient {count} lignes.")
    return count

@op
def transform_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    logger = get_dagster_logger()
    original_len = len(df)

    # 🔹 Supprimer les doublons
    df_clean = df.drop_duplicates()
    after_dedup = len(df_clean)

    # 🔹 Supprimer les lignes contenant des valeurs nulles
    df_clean = df_clean.dropna()
    after_dropna = len(df_clean)

    # 🔹 Tenter un tri automatique
    tri_colonnes = ["matricule", "id", "ID", "code", "date"]  # à adapter selon ton fichier
    colonne_de_tri = next((col for col in tri_colonnes if col in df_clean.columns), None)

    if colonne_de_tri:
        df_clean = df_clean.sort_values(by=colonne_de_tri)
        logger.info(f"🔽 Données triées par la colonne '{colonne_de_tri}'.")
    else:
        logger.warning("⚠️ Aucune colonne trouvée pour trier les données.")

    # 🔹 Log d’aperçu
    logger.info(
        f"""🧼 Nettoyage terminé :
        - Lignes d'origine : {original_len}
        - Après suppression des doublons : {after_dedup}
        - Après suppression des valeurs nulles : {after_dropna}
        - Colonnes présentes : {list(df_clean.columns)}"""
            )

    return df_clean

@op
def write_to_duckdb(df: pd.DataFrame) -> bool:
    logger = get_dagster_logger()
    try:
        con = duckdb.connect("/opt/dagster/dagster_home/bareme_solde.duckdb")
        con.execute("DROP TABLE IF EXISTS bareme_solde")
        con.execute("CREATE TABLE bareme_solde AS SELECT * FROM df")
        tables = con.execute("SHOW TABLES").fetchall()
        logger.info("✅ Table écrite dans DuckDB. Tables présentes : " + str(tables))
        logger.info("✅ Table écrite dans DuckDB avec succès.")
        logger.info("📄 Liste des tables dans DuckDB : " + str(con.execute("SHOW TABLES").fetchall()))
        con.close()
    except Exception as e:
        logger.error(f"❌ Erreur lors de l’écriture dans DuckDB : {e}")
        raise
    return True  # juste pour déclencher la dépendance

@op
def load_from_duckdb(trigger: bool) -> pd.DataFrame:
    logger = get_dagster_logger()
    try:
        con = duckdb.connect("/opt/dagster/dagster_home/bareme_solde.duckdb")
        tables = con.execute("SHOW TABLES").fetchall()
        logger.info("📄 Tables disponibles dans DuckDB : " + str(tables))
        if ('bareme_solde',) not in tables:
            raise Exception("❌ La table 'bareme_solde' n'existe pas dans DuckDB.")
        df = con.execute("SELECT * FROM bareme_solde").df()
        logger.info("✅ Données chargées depuis DuckDB.")
        con.close()
        return df
    except Exception as e:
        logger.error(f"❌ Erreur lors de la lecture depuis DuckDB : {e}")
        raise

@op
def count_rows_duckdb(df: pd.DataFrame) -> int:
    count = len(df)
    get_dagster_logger().info(f"🔢 DuckDB contient {count} lignes.")
    return count

# -----------------------------
# Étape 3 : PostgreSQL
# -----------------------------

@op
def write_to_postgres(df: pd.DataFrame):
    logger = get_dagster_logger()
    try:
        engine = create_engine("postgresql://admin:admin@postgres:5432/datalake")
        with engine.connect() as conn:
            df.to_sql("bareme_solde", conn, if_exists="replace", index=False)
        logger.info("✅ Données insérées dans PostgreSQL.")
    except Exception as e:
        logger.error(f"❌ Erreur lors de l'insertion PostgreSQL : {e}")
        raise

@op
def read_from_postgres():
    logger = get_dagster_logger()
    try:
        engine = create_engine("postgresql://admin:admin@postgres:5432/datalake")
        with engine.connect() as conn:
            # Vérifier que la table existe
            result = conn.execute(text("SELECT to_regclass('public.bareme_solde')"))
            if result.scalar() is None:
                raise Exception("❌ La table 'bareme_solde' n'existe pas dans PostgreSQL.")
            df = pd.read_sql("SELECT * FROM bareme_solde", conn)
        logger.info("📥 Lecture depuis PostgreSQL réussie.\nAperçu :\n" + df.head().to_string())
        return df
    except Exception as e:
        logger.error(f"❌ Erreur lors de la lecture PostgreSQL : {e}")
        raise

@op
def read_count_from_postgres():
    logger = get_dagster_logger()
    try:
        engine = create_engine("postgresql://admin:admin@postgres:5432/datalake")
        with engine.connect() as conn:
            result = conn.execute(text("SELECT to_regclass('public.bareme_solde')"))
            if result.scalar() is None:
                raise Exception("❌ La table 'bareme_solde' n'existe pas dans PostgreSQL.")
            result = conn.execute(text("SELECT COUNT(*) FROM bareme_solde"))
            count = result.scalar()
        logger.info(f"📊 Il y a {count} lignes dans PostgreSQL.")
        return count
    except Exception as e:
        logger.error(f"❌ Erreur lors du comptage PostgreSQL : {e}")
        raise


# -----------------------------
# Étape 4 : Validation
# -----------------------------

@op
def validate_counts(minio_count: int, duckdb_count: int, postgres_count: int):
    logger = get_dagster_logger()
    if minio_count == duckdb_count == postgres_count:
        logger.info("✅ Validation réussie : tous les systèmes contiennent le même nombre de lignes.")
    else:
        logger.error(f"❌ Incohérence : MinIO={minio_count}, DuckDB={duckdb_count}, PostgreSQL={postgres_count}")
        raise Exception("Les comptes ne correspondent pas.")

# -----------------------------
# Définition des jobs Dagster
# # -----------------------------

# @job
# def full_pipeline_job():
#     df = load_from_minio()
#     df_clean = transform_dataframe(df)  # 🧼 étape de nettoyage
#     count_minio = count_rows_minio(df_clean)

#     duckdb_written = write_to_duckdb(df_clean)
#     df_duckdb = load_from_duckdb(duckdb_written)

#     count_duckdb = count_rows_duckdb(df_duckdb)
#     write_to_postgres(df_duckdb)
#     postgres_count = read_count_from_postgres()

#     validate_counts(count_minio, count_duckdb, postgres_count)
@job
def full_pipeline_job():
    df = load_from_minio()
    raw_count = count_rows_minio(df)        # <--- ici : données brutes
    df_clean = transform_dataframe(df)
    duckdb_written = write_to_duckdb(df_clean)
    df_duckdb = load_from_duckdb(duckdb_written)
    count_duckdb = count_rows_duckdb(df_duckdb)
    write_to_postgres(df_duckdb)
    postgres_count = read_count_from_postgres()

    validate_counts(count_duckdb, count_duckdb, postgres_count)  # comparer post-nettoyage


@job
def test_postgres_write_and_read():
    df = load_from_minio()
    write_to_postgres(df)
    read_from_postgres()
    read_count_from_postgres()



# -----------------------------
# Export du job principal
# -----------------------------

etl_job = full_pipeline_job
# -----------------------------


#Créer un asset simple (bareme_solde_asset) qui représente le chargement des données depuis MinIO, 
# et programmer un schedule qui exécute le pipeline (full_pipeline_job) automatiquement chaque jour
#Un asset (actif de données) est une représentation d’une donnée concrète 
# ou d’un fichier (comme une table dans une base de données, 
# un fichier dans MinIO, etc.) qu’on peut produire ou transformer dans un pipeline
from dagster import asset, define_asset_job, ScheduleDefinition

@asset
def bareme_solde_asset() -> pd.DataFrame:
    logger = get_dagster_logger()
    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
    )
    try:
        obj = s3.get_object(Bucket="datatest", Key="bareme_solde.xlsx")
        df = pd.read_excel(BytesIO(obj["Body"].read()))
        logger.info("\U0001F4E6 Asset chargé depuis MinIO. Aperçu :\n" + df.head().to_string())
        return df
    except Exception as e:
        logger.error(f"❌ Erreur lors du chargement de l'asset : {e}")
        raise

# Définir un job basé sur l'asset
bareme_job = define_asset_job(name="bareme_job", selection=["bareme_solde_asset"])

#Un schedule est un déclencheur automatique basé sur le temps
#si on veut que  pipeline ETL s'exécute automatiquement tous les jours 
# pour récupérer les nouvelles données depuis MinIO et 
# les injecter dans DuckDB/Postgres, on utilise schedule
# Planification quotidienne à 07h00 (UTC)
daily_schedule = ScheduleDefinition(
    job=bareme_job,
    cron_schedule="0 7 * * *",  # tous les jours à 7h00
)  





