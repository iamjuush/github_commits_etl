import sys

import pandas as pd
import requests
import sqlalchemy
from loguru import logger

HEADERS = {"Accept": "application/vnd.github.v3+json"}


def get_repository_info(repository: str, organisation: str) -> pd.DataFrame:
    """
    Sends a GET request to github api to obtain repository id to be used as primary key in database.
    :return: pd.DataFrame
    """
    repo_url = f"https://api.github.com/repos/{organisation}/{repository}"
    response = requests.get(url=repo_url, headers=HEADERS)
    if not response.ok:
        logger.error(f"No results returned for {organisation}/{repository}. Error msg: {response.text}")
        sys.exit(1)

    repo_data = response.json()
    repo_df = pd.DataFrame({"repo_id": [repo_data["id"]], "organisation": [organisation], "repository": [repository]})
    return repo_df


def get_commits_info(organisation: str, repository: str, repo_id: int, start_date: str) -> pd.DataFrame:
    """
    Sends a GET request to github api to obtain commit and author info.
    :return: pd.DataFrame
    """
    commits_url = f"https://api.github.com/repos/{organisation}/{repository}/commits"

    commits_data = get_raw_commit_data(commits_url, start_date)
    if len(commits_data) == 0:
        logger.info("No commits found. Terminating program.")
        sys.exit(0)

    commit_df = make_commits_dataframe(commits_data, repo_id=repo_id)
    commit_df.dropna(how="any", inplace=True)  # rows will be nan in event of author with no id
    return commit_df


def make_commits_dataframe(commits_data: list, repo_id: int) -> pd.DataFrame:
    # Need to take care of cause where author has no data inside,
    # so decision is to omit that author is author has no id.
    commits = [{"id": obj["sha"],
                "timestamp": obj["commit"]["author"]["date"],
                "name": obj["commit"]["author"]["name"],
                "author_id": obj["author"]["id"],
                "repo_id": repo_id,
                "message": obj["commit"]["message"],
                "url": obj["html_url"]
                } if obj["author"] is not None else {} for obj in commits_data]
    commit_df = pd.DataFrame(commits)
    commit_df["timestamp"] = pd.to_datetime(commit_df["timestamp"])
    return commit_df


def get_raw_commit_data(commits_url: str, start_date: str) -> list:
    exists_new_data = True  # Flag to indicate if there is any data
    page_number = 1
    commits_data = []

    # Iterate thru the pages till we dont get anymore data new data.
    while exists_new_data:
        logger.info(f"Fetching commits data from page num: {page_number}")
        new_data = requests.get(url=commits_url,
                                headers=HEADERS,
                                params={"since": start_date, "per_page": 100, "page": page_number}).json()
        if len(new_data) == 0:
            logger.info("No more data found")
            exists_new_data = False
        else:
            commits_data = commits_data + new_data
            page_number += 1
    return commits_data


def load_to_db(engine: sqlalchemy.engine, **dataframes):
    """
    1) Inserts the tables into the database staging schema.
    2) Run stored procedure to upsert from staging into main schema
    :param engine: sqlalchemy.engine
    :param dataframes: dict
    """
    # Insert into staging
    for table_name, df in dataframes.items():
        logger.info(f"Inserting into staging.{table_name}")
        df.to_sql(con=engine, name=table_name, schema="staging", if_exists="replace", index=False)

    # Do upsert from staging into main schema by calling the stored procedure
    with engine.begin() as conn:
        logger.info("Executing upsert stored procedure")
        conn.execute("call staging.upsert_all();")