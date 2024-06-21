from data.professions_list import prof_list
import logging
from airflow.decorators import task


@task
def get_professions():
    try:
        professions: list = prof_list

        logging.info("Got the list of professions")

        return professions
    
    except Exception as e:
        logging.error("Error during getting professions:", e)