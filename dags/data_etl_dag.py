from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
import os
from tasks.get_professions import get_professions
from tasks.parse_hh import get_dataset_with_tags
from airflow.utils.task_group import TaskGroup
from tasks.data_preprocessing.preprocess_tags import preprocess_tags
from tasks.data_preprocessing.filter_tags import filter_tags
from tasks.data_preprocessing.convert_to_matrix import convert_to_matrix
from airflow.models import Variable


def get_dir():
    print(os.getcwd())


default_args = {
    "owner": "Solarev Ruslan",
    "retries": 5,
    "retry_delay": timedelta(minutes=5)
}


with DAG(
    dag_id="dag_data_etl_v54",
    description="It is a dag which gets data from HeadHunter and preprocesses them",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 0,12 * * *",
    catchup=False,
    default_args=default_args
) as dag:
    max_row_count = int(Variable.get("max_row_count", 5))
    card_type = Variable.get("card_type", default_var="vacancy")

    with TaskGroup("data_etl") as data_etl:

        with TaskGroup("parsing_data") as parsing_data:
            professions = get_professions()
            get_dataset_with_tags(max_row_count, card_type, professions)

        with TaskGroup("preprocessing_data") as preprocessing_data:
            preprocess_data_task = PythonOperator(
                task_id="preprocess_data",
                python_callable=preprocess_tags
            )

            filter_tags_task = PythonOperator(
                task_id="filter_tags",
                python_callable=filter_tags
            )

            convert_to_matrix_task = PythonOperator(
                task_id="convert_to_matrix",
                python_callable=convert_to_matrix
            )
        
        parsing_data >> preprocess_data_task >> filter_tags_task >> convert_to_matrix_task