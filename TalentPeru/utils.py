def save_data(data, location, num_works):
    data = pd.DataFrame(data)
    location = limpiar_texto(location)
    dep_dir = f"./data/{location}"
    dep_last_scrapper = f"{dep_dir}/{today}.csv"
    try:
        os.makedirs(dep_dir)
    except:
        pass

    data.to_csv(dep_last_scrapper, index=False)
    into_log(location, today, num_works)


PATH_LOG = "./data_logs/logs.csv"


def into_log(dep, date, n_jobs_text) -> None:
    last_log = pd.read_csv(PATH_LOG)
    path_location = f"/data/{dep}/{date}.csv"
    n = extract_njobs(n_jobs_text)
    data = {
        "dep": [dep],
        "last_date": [date],
        "path_data": [path_location],
        "n_jobs_text": [n_jobs_text],
        "n_jobs": [n],
    }
    pd.concat([last_log, pd.DataFrame(data)]).to_csv(PATH_LOG, index=False)
