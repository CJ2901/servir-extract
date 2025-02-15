from concurrent.futures import ThreadPoolExecutor, as_completed
from TalentPeru.Scrapper_jobs.with_requests import (
    first_session,
    left_to_rigth,
    right_to_left,
)
from time import time
import pandas as pd
import pytz, subprocess
import datetime, uuid

gmt5 = pytz.timezone("Etc/GMT+5")
today = datetime.datetime.now(gmt5).strftime("%d-%m-%Y")

deps = [str(n).zfill(2) for n in range(1, 26) if n != 15]
lima = "15"
PATH_LOG = "./data_logs/logs.csv"
data_path = f"./data/all/{today}.csv"
# print(deps)


def run_git_commands(total_time):

    subprocess.run(["git", "add", "-A"], check=True)

    commit_message = f"Data with requests {today}, total time: {total_time} seconds"
    subprocess.run(["git", "pull"], check=True)
    subprocess.run(["git", "commit", "-m", commit_message], check=True)

    subprocess.run(["git", "push"], check=True)


if __name__ == "__main__":
    start = time()

    def ejecutar(dep):
        session, first_page_soup, view_state_value = first_session()
        data_dep = left_to_rigth(
            dep=dep, view_state_value=view_state_value, session=session
        )
        return data_dep

    def both_sides(right=True):
        session, first_page_soup, view_state_value = first_session()
        if right:
            data_dep = left_to_rigth(
                dep=lima,
                view_state_value=view_state_value,
                session=session,
                right=right,
            )
            return data_dep
        else:
            data_dep = right_to_left(
                dep=lima, view_state_value=view_state_value, session=session
            )
            return data_dep

    data_dep_list = []

    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_dep = {executor.submit(ejecutar, dep): dep for dep in deps}

        for future in as_completed(future_to_dep):
            data_dep_list.append(future.result())

    with ThreadPoolExecutor(max_workers=2) as executor:
        future_to_dep = {
            executor.submit(both_sides, right_side): right_side
            for right_side in [True, False]
        }

        for future in as_completed(future_to_dep):
            data_dep_list.append(future.result())

    data = pd.concat(data_dep_list, ignore_index=True)
    data["id_uuid"] = data.apply(lambda row: uuid.uuid4(), axis=1)
    data.to_csv(data_path, index=False)
    last_log = pd.read_csv(PATH_LOG)
    end = time() - start
    data_log = {
        "dep": ["all"],
        "last_date": today,
        "path_data": [data_path],
        "n_jobs_text": [len(data) - 10],
        "n_jobs": [len(data) - 10],
        "time": [end],
    }
    day_data = pd.concat([last_log, pd.DataFrame(data_log)], ignore_index=True)
    day_data["id_uuid"] = day_data.apply(lambda row: uuid.uuid4(), axis=1)
    day_data.to_csv(PATH_LOG, index=False)


    # run_git_commands(end)

    print(f"Tiempo de ejecución: {end } segundos")
