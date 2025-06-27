from dagster import op, job

@op
def hello():
    return "Salut depuis Dagster !"

@job
def hello_job():
    hello()
