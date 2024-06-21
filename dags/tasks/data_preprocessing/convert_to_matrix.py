import pandas as pd
import os
from airflow.models import Variable
import logging


def save_matrix(path: str, data):
    try:
        if os.path.exists(path):
            dataset = pd.read_csv(path)
            data = pd.concat([dataset, data])

        data.to_csv(path, index=False)
    
    except Exception as e:
        logging.error("Error during saving matrix:", e)


def convert_to_matrix():
    try:
        preproc_data_path = Variable.get("preprocessed_data_path", 
                                        default_var="dags/data/Preprocessed_dataset.csv")
        top_tags_path = Variable.get("top_tags_path", default_var="dags/data/Top_tags.csv")
        skill_matrix_path = Variable.get("skill_matrix_path", default_var="dags/data/Skill_matrix.csv")


        dataset = pd.read_csv(preproc_data_path)
        dataset["Tags"] = dataset["Tags"].apply(lambda x: eval(x))
        print(dataset.columns)
        top_tags = pd.read_csv(top_tags_path)["Skill"].to_list()

        user_skill_matrix = pd.DataFrame(columns=top_tags)

        for index, row in dataset.iterrows():
            tags = row["Tags"]
            for tag in tags:
                if tag in top_tags:
                    user_skill_matrix.loc[index, tag] = 1
            print(f"{index}/{len(dataset)}")
        
        user_skill_matrix.fillna(0,inplace=True)

        save_matrix(path=skill_matrix_path, data=user_skill_matrix)

        logging.info("Task convert_to_matrix was successfully completed")

        return

    except Exception as e:
        logging.error("Error during converting to matrix:", e)