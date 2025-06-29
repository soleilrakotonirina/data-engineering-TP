from dagster import repository
from .etl_pipeline import (
    etl_job,
    bareme_solde_asset,
    bareme_job,
    daily_schedule
)

@repository
def my_repo():
    return [
        etl_job,
        bareme_solde_asset,
        bareme_job,
        daily_schedule,
    ]
