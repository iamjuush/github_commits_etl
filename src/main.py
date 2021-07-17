import pandas as pd
from sqlalchemy import create_engine

from src.extractor import get_repository_info, get_commits_info, load_to_db
from src.heatmap import df_to_heatmap


def run_etl(organisation: str, repository: str, start_date: str):
    # Generate URLS

    # Obtain data using various api
    repo_df = get_repository_info(repository=repository,
                                  organisation=organisation)
    commit_df = get_commits_info(repository=repository,
                                 organisation=organisation,
                                 start_date=start_date,
                                 repo_id=repo_df["repo_id"].iloc[0])
    author_df = commit_df[["author_id", "name"]]
    dataframes = {
        "repositories_dim": repo_df,
        "commit_df": commit_df,
        "author_df": author_df
    }

    # Drop name column because we will use author_id to link to authors dimension table
    commit_df.drop(columns=["name"], inplace=True)
    load_to_db(ENGINE, **dataframes)


def make_heatmap(organisation: str, repository: str):
    sql = f"""
            SELECT count(id) commit_counts, DAYOFWEEK(DATE(timestamp)) day_of_week, floor(hour(time(timestamp)) / 3) interval_of_day 
            FROM commits_fact
            INNER JOIN repositories_dim rd ON commits_fact.repo_id = rd.repo_id
            WHERE repository = '{repository}' AND organisation = '{organisation}'
            GROUP BY day_of_week, interval_of_day 
    """
    df = pd.read_sql(con=ENGINE, sql=sql)
    heatmap = df_to_heatmap(df)
    print(heatmap.to_markdown())


if __name__ == "__main__":
    ENGINE = create_engine("mysql+pymysql://root:password123@localhost:3306/github_etl")
    # Get user inputs
    MODE = input("==Choose 1 or 2== \n"
                 "1) Run ETL \n"
                 "2) Make heatmap: ") or "1"
    ORGANISATION = input("Organisation [apache]: ") or "apache"
    REPOSITORY = input("Repository [hadoop]: ") or "hadoop"
    if MODE == "1":
        START_DATE = input("Start date [2021-01-01T00:00:00Z]: ") or "2021-01-01T00:00:00Z"
        run_etl(ORGANISATION, REPOSITORY, START_DATE)
    elif MODE == "2":
        make_heatmap(ORGANISATION, REPOSITORY)