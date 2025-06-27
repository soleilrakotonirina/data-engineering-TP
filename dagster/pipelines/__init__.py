from dagster import repository
from .etl_pipeline import etl_job
from .hello_job import hello_job

@repository
def my_repo():
    return [etl_job, hello_job]
