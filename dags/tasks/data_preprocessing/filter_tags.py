import pandas as pd
import os
from airflow.models import Variable
import logging


def has_matching_tag(tags, top_tags):
    return any(tag in top_tags for tag in tags)


def save_dataset(path: str, data):
    try:
        data.to_csv(path, index=False)
    
    except Exception as e:
        logging.error("Error during saving dataset:", e)


def add_to_final_dataset(path: str, data):
    try:
        if os.path.exists(path):
            dataset = pd.read_csv(path)
            data = pd.concat([dataset, data])

        data.to_csv(path, index=False)
    
    except Exception as e:
        logging.error("Error during adding to final dataset:", e)



def filter_tags():
    try:
        preproc_data_path = Variable.get("preprocessed_data_path", 
                                        default_var="dags/data/Preprocessed_dataset.csv")
        top_tags_path = Variable.get("top_tags_path", default_var="dags/data/Top_tags.csv")
        hh_dataset_path = Variable.get("hh_dataset_path", default_var="dags/data/HH_Dataset.csv")
        
        dataset = pd.read_csv(preproc_data_path)

        dataset["Tags"] = dataset["Tags"].apply(lambda x: eval(x))

        top_tags = pd.read_csv(top_tags_path)["Skill"].to_list()

        dataset["Tags"] = dataset["Tags"].apply(
            lambda x: [element for element in x if element in top_tags])
        

        # Применяем фильтрацию
        dataset = dataset[dataset['Tags'].apply(lambda x: has_matching_tag(x, top_tags))]


        dataset.reset_index(drop=True, inplace=True)

        save_dataset(path=preproc_data_path, data=dataset)

        add_to_final_dataset(path=hh_dataset_path, data=dataset)

        logging.info("Task filter_tags was successfully completed")
    
    except Exception as e:
        logging.error("Error during filtering tags:", e)