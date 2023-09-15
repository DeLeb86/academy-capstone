from airflow import DAG
from pendulum import datetime
from airflow.providers.amazon.aws.operators.batch import BatchOperator
#from datetime import date,datetime as dt
import os
#import boto3
ECR="338791806049.dkr.ecr.eu-west-1.amazonaws.com/denis:capstone-1.0.1"
#def get_image_tag() -> str :
#    ecr=boto3.client("ecr")
#    tags=[v for x in ecr.list_images(repositoryName="denis")["imageIds"] for k,v in x.items() if "capstone" in v]
#    return sorted(tags,reverse=True)[0]


dag = DAG(
    dag_id="denis-capstone-project",
    description="trigger capstone data from s3 to snowflake",
    default_args={"owner": "Airflow"},
    schedule_interval="@daily",
    start_date=datetime(2023,9,15,tz="Europe/Brussels"),
)



scheduled_task = BatchOperator(
    task_id="ingestor-capstone",
    job_definition="Denis-capstone-job",
    job_queue="academy-capstone-summer-2023-job-queue",
    job_name="denis-execution",
    dag=dag)
