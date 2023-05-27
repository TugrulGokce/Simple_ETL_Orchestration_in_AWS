from airflow import DAG
import boto3
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from time import sleep

# initialize glue client
glue = boto3.client('glue')

# glue job environment variable
glue_bucket_name = "airflow-envv"
glue_job_name = 'yelp-airflow-glue-job'
glue_iam_role_name = 'notebookk'
yelp_catalog_db_name = "glue-yelp-review-db"

# processed yelp data s3 bucket path
review_processed_s3_bucket_name = "s3://glue-yelp-output/yelp-review-processed/"
user_processed_s3_bucket_name = "s3://glue-yelp-output/yelp-user-processed/"
business_processed_s3_bucket_name = "s3://glue-yelp-output/yelp-business-processed/"

# Athena environment variable
athena_create_table_sql = """
CREATE TABLE IF NOT EXISTS "glue-yelp-review-db"."athena_output_table"
WITH (format = 'TEXTFILE', field_delimiter = ',') AS
SELECT user.user_id,
  user.user_name,
  user.review_count_of_user,
  user.cool,
  review.review_id,
  review.useful,
  review.stars_of_review,
  review.sentiment_analysis_of_review,
  business.business_id,
  business.business_name,
  business.city,
  business.state,
  business.postal_code,
  business.latitude,
  business.longitude,
  business.review_count_of_business,
  business.stars_of_business
FROM "yelp_business_processed" as business
  JOIN "yelp_review_processed" as review ON business.business_id = review.business_id
  JOIN "yelp_user_processed" as user ON review.user_id = user.user_id;
"""
athena_output_bucket = "s3://yelp-athena-query-outputs-bucket/athena_airflow_output/"


def starting_dag():
    print("Starting DAG")


def ending_dag():
    print("Ending DAG")


def create_glue_database(glue_catalog_db_name: str):
    glue.create_database(
        DatabaseInput={
            'Name': glue_catalog_db_name,
            'Description': 'This database created for glue yelp-review data.',
        }
    )

    print(f"{glue_catalog_db_name} glue catalog database created successfully.")


def create_crawler_and_run(crawler_name: str, crawler_s3_path_name: str):
    # create Crawler
    glue.create_crawler(
        Name=crawler_name,
        Role='arn:aws:iam::991694788150:role/notebookk',
        DatabaseName=yelp_catalog_db_name,
        Description='This crawler, crawl all files under s3://glue-yelp-output/yelp-business-processed/',
        Targets={
            'S3Targets': [
                {
                    'Path': crawler_s3_path_name
                }
            ]
        },
        SchemaChangePolicy={
            'DeleteBehavior': 'LOG',
        },
        RecrawlPolicy={
            'RecrawlBehavior': 'CRAWL_EVERYTHING'
        },
    )

    print(f"{crawler_name} created successfully.")

    # start Crawler
    glue.start_crawler(
        Name=crawler_name
    )

    print(f"{crawler_name} started.")

    # wait Glue Crawler until completed
    while True:
        response_crawler = glue.get_crawler(Name=crawler_name)
        crawler_state = response_crawler['Crawler']['State']

        print(f"{crawler_state=}")
        if crawler_state == 'READY':
            break
        sleep(10)

    print(f"{crawler_name} crawler completed successfully.")


def create_glue_job(bucket_name: str, job_name: str, iam_role_name: str):
    # create glue job
    glue.create_job(
        Name=job_name,
        Role=iam_role_name,
        ExecutionProperty={
            'MaxConcurrentRuns': 1
        },
        Command={
            'Name': 'glueetl',
            'ScriptLocation': f's3://{bucket_name}/yelp-glue-job.py',
            'PythonVersion': '3'
        },
        DefaultArguments={
            '--job-language': 'python',
            "--additional-python-modules": "awswrangler,s3://yelp-data-bucket1/flair-0.12.2-py3-none-any.whl"
        },
        GlueVersion='3.0',
        WorkerType='G.1X',
        NumberOfWorkers=2,
    )
    print(f"{job_name=} job created.")


def start_glue_job(job_name: str):
    # start glue job
    glue.start_job_run(JobName=job_name)
    print(f"{job_name=} job started.")

    # wait Glue Job until completed
    while True:
        response_job = glue.get_job_runs(JobName=job_name)
        glue_job_state = response_job["JobRuns"][0]['JobRunState']

        print(f"{glue_job_state=}")
        if glue_job_state == 'SUCCEEDED':
            break
        sleep(30)

    print(f"{job_name=} job completed successfully.")


default_args = {
    'owner': 'tudimudi',
    'start_date': datetime(2023, 5, 23),
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    dag_id='glue_operator_example',
    description='Example DAG using Glue Operator',
    start_date=datetime(2023, 5, 23),
    schedule_interval=None,
    default_args=default_args,
    catchup=False
)

create_glue_job_task = PythonOperator(
    task_id="create_glue_job",
    python_callable=create_glue_job,
    op_kwargs={
        "bucket_name": glue_bucket_name,
        "job_name": glue_job_name,
        "iam_role_name": glue_iam_role_name
    },
    dag=dag
)

start_glue_job_task = PythonOperator(
    task_id="start_glue_job",
    python_callable=start_glue_job,
    op_kwargs={
        "job_name": glue_job_name
    }
)

create_glue_catalog_db = PythonOperator(
    task_id="create-yelp-glue-catalog-db",
    python_callable=create_glue_database,
    op_kwargs={"glue_catalog_db_name": yelp_catalog_db_name},
    dag=dag
)

review_crawler = PythonOperator(
    task_id="review-crawler-start",
    python_callable=create_crawler_and_run,
    op_kwargs={"crawler_name": "review-crawler",
               "crawler_s3_path_name": review_processed_s3_bucket_name},
    dag=dag
)

business_crawler = PythonOperator(
    task_id="business-crawler-start",
    python_callable=create_crawler_and_run,
    op_kwargs={"crawler_name": "business-crawler",
               "crawler_s3_path_name": business_processed_s3_bucket_name},
    dag=dag
)

user_crawler = PythonOperator(
    task_id="user-crawler-start",
    python_callable=create_crawler_and_run,
    op_kwargs={"crawler_name": "user-crawler",
               "crawler_s3_path_name": user_processed_s3_bucket_name},
    dag=dag
)

athena_table_task = AthenaOperator(
    task_id="create_athena_table",
    query=athena_create_table_sql,
    database=yelp_catalog_db_name,
    output_location=athena_output_bucket
)

create_glue_job_task >> start_glue_job_task >> create_glue_catalog_db
create_glue_catalog_db >> [review_crawler, business_crawler, user_crawler] >> athena_table_task
