# URL_BASE = "https://raw.githubusercontent.com/TJhon/talento_peru_jobs/refs/heads/main/data/{location}/{date}.csv"

import pandas as pd, tqdm

LOGS = "./data/logs/logs.csv"

# url = URL_BASE.format(location=12, date=2)

# df = pd.read_csv(LOGS)
last_date_col = "last_date_format"
# df[last_date_col] = pd.to_datetime(df["last_date"], dayfirst=True)

last_date_col = "last_date_format"

def convert_to_local_path(dep, date):
    # Define la ruta local directamente en lugar de usar URL_BASE
    return f"./data/{dep}/{date}.csv"

def days_scrapper(df):
    day_scrapper = df.sort_values(last_date_col, ascending=False)[
        last_date_col
    ].unique()
    return day_scrapper


def last_scrapper(df: pd.DataFrame, regions: list = None) -> pd.DataFrame:
    if regions is not None:
        df = df.query("dep in @regions")
    last_dates = df.loc[df.groupby("dep")[last_date_col].idxmax()]
    regions = last_dates["dep"]

    paths_to_data = []

    for i, row in last_dates.iterrows():
        paths_to_data.append(convert_to_local_path(row["dep"], row["last_date"]))
    data_array = []
    for path, dep in zip(paths_to_data, last_dates["dep"]):
        df = read_data(path, dep)
        data_array.append(df)

    return pd.concat(data_array, ignore_index=True)


# def convert_to_url_csv(location, date):
#     url_csv = URL_BASE.format(location=location, date=date)
#     return url_csv


def read_data(url, dep):
    github_data = pd.read_csv(url)
    github_data["dep"] = dep
    return github_data


# last_scrapper()
# def get_data(urls) -> pd.DataFrame:
#     data_array = []
#     for url in tqdm.tqdm(urls):

#         data_array.append(github_data)

#     data = pd.concat(data_array, ignore_index=True)
#     return data
