import pandas as pd, tqdm

from .utils import job_wage_float

URL_BASE = "https://raw.githubusercontent.com/TJhon/talento_peru_jobs/refs/heads/main/data/all/{date}.csv"

LOGS = "https://raw.githubusercontent.com/TJhon/talento_peru_jobs/refs/heads/main/data_logs/logs.csv"


def get_logs():
    # retorna los logs
    df_logs = pd.read_csv(LOGS)
    last_date_col = "last_date_format"
    df_logs[last_date_col] = pd.to_datetime(df_logs["last_date"], dayfirst=True)
    last_scrapper = df_logs.sort_values(last_date_col, ascending=False).head(1)
    data = {
        "last_scrapper_date": last_scrapper.to_dict("records")[0]["last_date"],
        "origin": LOGS,
        # "data": df_logs.to_dict("records"),
        "dates": list(df_logs["last_date"].unique()),
        # "dates": df_logs.to_dict("list")["last_date"],
        # }
    }

    return data


def clean_jobs_data(data: pd.DataFrame):
    ubication_col = ["ubication_region", "ubication_dist"]
    data[ubication_col] = data["ubication"].str.split("-", expand=True, n=1)
    for u in ubication_col:
        data[u] = data[u].str.strip()

    data["salary"] = data["salary"].apply(job_wage_float)
    return data


def get_jobs_data(date):
    if date not in get_logs()["dates"]:
        return {"error": "date not found in DB, see '/jobs/logs'"}
    data = pd.read_csv(URL_BASE.format(date=date))
    data = clean_jobs_data(data)
    return data.to_dict("records")


def get_last_jobs_data():
    last_date = get_logs()["last_scrapper_date"]
    data = get_jobs_data(last_date)
    return data
