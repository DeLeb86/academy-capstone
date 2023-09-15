from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from pendulum import datetime
from datetime import date,datetime as dt
import os
"""
Exercise 2.5

Extend your previous result to also print your age.
"""


MY_NAME = "Barack Obama"
MY_BIRTHDAY = datetime(year=1961, month=8, day=4, tz="Pacific/Honolulu")

dag = DAG(
    dag_id="happy_birthday_full_v3",
    description="Wishes you a happy birthday",
    default_args={"owner": "Airflow"},
    schedule_interval="0 0 4 8 *",
    start_date=MY_BIRTHDAY,
)


def years_today(dag,ds):
    print(ds)
    print(f"Congrats, you're {dt.strptime(ds,'%Y-%m-%d').year - dag.start_date.year}")
    """Returns how old you are at this moment"""
    #dt_=dt.strptime(os.getenv("DS"),"%Y-%m-%d")
    return 0


birthday_greeting = PythonOperator(
    task_id="send_wishes",
    dag=dag,
    python_callable=years_today
#    bash_command=(
#        f"echo 'Happy birthday, {MY_NAME}! "
#        #"You are {{ macros.datetime.strptime(ds,'%Y-%m-%d').year - dag.start_date.year}} years old today!'"
#        f"You are {years_today()}"
#    ),
)