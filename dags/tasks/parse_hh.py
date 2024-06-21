from os import name
import requests
import fake_useragent
from bs4 import BeautifulSoup
import time
import json
import logging
import pandas as pd
import os
from airflow.models import Variable
from airflow.decorators import task


def get_links(profession: str, max_row_count: int, card_type: str = "resume", existing_links: list = []):
    ua =fake_useragent.UserAgent()

    count = 0

    if card_type == "vacancy":
        try:
            res = requests.get(
                url=f"https://hh.ru/search/vacancy?text={profession}&from=suggest_post&salary=&ored_clusters=true&page=1",
                headers={"user-agent":ua.random}
            )

            if res.status_code != 200:
                logging.error("Couldn't connect to the site!")

                return
        
        except requests.exceptions.ConnectionError:
            logging.error("Connection error!")
        
        soup = BeautifulSoup(res.content, "lxml")

        try:
            page_count = int(soup.find("div", attrs={"class": "pager"}).find_all("span", recursive=False)[-1].find("a").find("span").text)
        except:
            return
        

        for page in range(page_count):
            try:
                res = requests.get(
                    url=f"https://hh.ru/search/vacancy?text={profession}&from=suggest_post&salary=&ored_clusters=true&page={page}",
                    headers={"user-agent":ua.random}
                )

                if res.status_code != 200:
                    logging.error("Couldn't connect to the site!")

                    return
        
            except requests.exceptions.ConnectionError:
                logging.error("Connection error!")

            soup = BeautifulSoup(res.content, "lxml")

            h2_tags = soup.find_all("h2", attrs={"data-qa": "bloko-header-2"})

            for h2_tag in h2_tags:
                try: 
                    link = h2_tag.find("a", attrs={"class": "bloko-link"}).attrs["href"].split("?")[0]

                    if "vacancy" in link and link not in existing_links:
                        count += 1
                        if count > max_row_count:
                            break

                        yield link
                except:
                    logging.error("Error: the other element was given")
            
            if count > max_row_count:
                break

    
    elif card_type == "resume":
        try:
            res = requests.get(
                url=f"https://hh.ru/search/resume?text={profession}&exp_period=all_time&logic=normal&pos=full_text&page=1",
                headers={"user-agent":ua.random}
            )

            if res.status_code != 200:
                logging.error("Couldn't connect to the site!")

                return
            
        except requests.exceptions.ConnectionError:
            logging.error("Connection error!")

        soup = BeautifulSoup(res.content, "lxml")

        try:
            page_count = int(soup.find("div", attrs={"class": "pager"}).find_all("span", recursive=False)[-1].find("a").find("span").text)
        except:
            return

        for page in range(page_count):
            try:
                res = requests.get(
                    url=f"https://hh.ru/search/resume?text={profession}&exp_period=all_time&logic=normal&pos=full_text&page={page}",
                    headers={"user-agent":ua.random}
                )

                if res.status_code != 200:
                    logging.error("Couldn't connect to the site!")

                    return
            
            except requests.exceptions.ConnectionError:
                logging.error("Connection error!")
            
            soup = BeautifulSoup(res.content, "lxml")

            h3_tags = soup.find_all("h3", attrs={"data-qa": "bloko-header-3"})
            
            for h3_tag in h3_tags:
                try:
                    link = h3_tag.find("a", attrs={"class": "bloko-link"}).attrs["href"].split("?")[0]

                    if "resume" in link and link not in existing_links:
                        count += 1
                        if count > max_row_count:
                            break

                        print(f"{count}/{max_row_count}")

                        yield f"https://hh.ru{link}"

                except Exception as e:
                    logging.error(f"Error during getting links: {e}")

            if count > max_row_count:
                break


def get_tags(link: str, card_type: str) -> list | None:
    ua =fake_useragent.UserAgent()

    try:
        res = requests.get(
            url=link,
            headers={"user-agent":ua.random}
        )

        if res.status_code != 200:
            logging.error("Couldn't connect to the site!")

            return
        
    except requests.exceptions.ConnectionError:
        logging.error("Connection error!")
        
    soup = BeautifulSoup(res.content, "lxml")
    
    if card_type == "vacancy":
        try:
            skills = soup.find_all("li", attrs={"data-qa": "skills-element"})
            
            if skills:
                tags = [skill.text for skill in skills]
                return tags
            
            else:
                return None

        except Exception as e:
            logging.error("Error during getting tags:", e)
            return None
    
    elif card_type == "resume":
        try:
            skills = soup.find("div", attrs={"class": "bloko-tag-list"}).find_all("span", attrs={"data-qa": "bloko-tag__text"})
            
            if skills:
                tags = [skill.text for skill in skills]
                return tags
            
            else:
                return None

        except Exception as e:
            logging.error("Error during getting tags:", e)
            return None


def get_prof_tags(profession: str, max_row_count: int, card_type: str, existing_links: list):
    links = get_links(profession, max_row_count, card_type, existing_links)

    skills_df = pd.DataFrame(columns=["Profession", "Link", "Tags"])

    for link in links:
        try:
            tags = get_tags(link, card_type)

            skills_df.loc[len(skills_df.index)] = [profession, link, tags]

        except Exception as e:
            logging.error(f"Error during getting tags: {e}")
        
        time.sleep(0.2)
    

    return skills_df

def save_dataset(path: str, data):
    try:
        data.to_csv(path, index=False)

    except Exception as e:
        logging.error("Error during saving dataset:", e)


@task
def get_dataset_with_tags(max_row_count: int, card_type: str, professions: list) -> pd.DataFrame:
    hh_dataset_path = Variable.get("hh_dataset_path", default_var="dags/data/HH_Dataset.csv")
    parsed_data_path = Variable.get("parsed_data_path", default_var="dags/data/Parsed_data.csv")

    try:
        if os.path.exists(hh_dataset_path):
            existing_links = pd.read_csv(hh_dataset_path)["Link"].to_list()
        else:
            existing_links = []

        dataset = pd.DataFrame(columns=["Profession", "Link", "Tags"])
    
    except Exception as e:
        logging.error("Error during getting existing_links:", e)

    try:
        for profession in professions:
            logging.info(f"New profession was added: {profession}")

            prof_df = get_prof_tags(profession, max_row_count, card_type, existing_links)

            dataset = pd.concat([dataset, prof_df])

    except Exception as e:
        logging.error(f"Error: {e}")

    save_dataset(path=parsed_data_path, data=dataset)

    logging.info("task parsing_data was successfully completed")
