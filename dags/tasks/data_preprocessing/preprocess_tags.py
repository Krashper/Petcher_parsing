import pandas as pd
import re
from airflow.models import Variable
import logging


def convert_string_to_list(string):
    try:
        # Удаляем квадратные скобки
        string = string.strip("[]")
        # Разделяем строку по шаблону, который учитывает пробелы и переводы строки
        tags_list = re.split(r"\s+'\s*", string)
        # Удаляем пустые строки и очищаем от лишних символов
        cleared_tags_list = [tag.strip().replace("'", "").replace(",", "") for tag in tags_list if tag.strip()]
        return cleared_tags_list
    
    except Exception as e:
        logging.error("Error during converting strings to lists:", e)


def save_dataset(path: str, data):
    try:
        data.to_csv(path, index=False)
    
    except Exception as e:
        logging.error("Error during saving dataset:", e)



def preprocess_tags():
    try:
        parsed_data_path = Variable.get("parsed_data_path", default_var="dags/data/Parsed_data.csv")
        preproc_data_path = Variable.get("preprocessed_data_path", 
                                        default_var="dags/data/Preprocessed_dataset.csv")

        dataset = pd.read_csv(parsed_data_path)

        dataset = dataset.fillna("[]")

        dataset['Tags'] = dataset['Tags'].apply(convert_string_to_list)

        dataset["Tags"] = dataset["Tags"].map(lambda x: x if isinstance(x, list) else [])
    
    except Exception as e:
        logging.error("Error during preprocessing tags:", e)

    save_dataset(path=preproc_data_path, data=dataset)

    logging.info("Task preprocess_data was successfully completed")