from dagster import job, op, repository

@op
def hello():
    return "Salut depuis Dagster !"

@job
def hello_job():
    hello()

@repository
def my_repo():
    return [hello_job]
