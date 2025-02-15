from concurrent.futures import ThreadPoolExecutor, as_completed
from TalentAPI.init_supabase import upload_and_drop_data

from TalentPeru.Scrapper_jobs.detailed_jobs import JobScrapper

from TalentPeru.Scrapper_jobs.api_jobs import clean_jobs_data
from time import time
import pandas as pd
import pytz, subprocess
import datetime

gmt5 = pytz.timezone("Etc/GMT+5")
today = datetime.datetime.now(gmt5).strftime("%d-%m-%Y")


deps = [str(n).zfill(2) for n in range(1, 26) if n != 15]
# deps = [str(n).zfill(2) for n in [1, 3] if n != 15]
lima = "15"
# lima = "25"
PATH_LOG = "./data_logs/logs.csv"
data_path = f"./data/all/{today}.csv"


columns = {
    "url_convocatoria": "job_posting_url",
    "Cantidad De Vacantes:": "vacancies",
    "Número De Convocatoria:": "job_posting_number",
    "Remuneración:": "salary",
    "Fecha Inicio De Publicación:": "start_publication_date",
    "Fecha Fin De Publicación:": "end_publication_date",
    "Experiencia:": "required_experience",
    "Formación Académica - Perfil:": "educational_background",
    "Especialización:": "specialization",
    "Conocimiento:": "required_knowledge",
    "Competencias:": "skills",
    "position": "job_title",
    "institution": "public_institution",
    "uuid": "unique_id",
    "day_scrapper": "scraping_date",
}


def run_git_commands(total_time):
    # pass
    subprocess.run(["git", "add", "-A"], check=True)

    commit_message = f"Data with requests {today}, total time: {total_time} seconds"
    subprocess.run(["git", "pull"], check=True)
    subprocess.run(["git", "commit", "-m", commit_message], check=True)

    subprocess.run(["git", "push"], check=True)


if __name__ == "__main__":
    start = time()

    def ejecutar(dep):
        data_dep = JobScrapper(dep=dep).scrapper_sequential()
        return data_dep

    def both_sides(right=True):  # lima
        if right:
            data_dep = JobScrapper(dep=lima).scrapper_both_right()
        else:
            data_dep = JobScrapper(dep=lima).scrapper_both_left()
        return data_dep

    data_dep_list = []

    # Ejecutar both_sides en paralelo junto con ejecutar(dep)
    with ThreadPoolExecutor(
        max_workers=10
    ) as executor:  # max_workers aumentado para incluir both_sides
        # Crear futuros para deps
        future_to_dep = {
            executor.submit(both_sides, right_side): f"both_sides_{right_side}"
            for right_side in [True, False]
        }

        # Añadir both_sides a los futuros
        future_to_dep.update({executor.submit(ejecutar, dep): dep for dep in deps})

        # Recolectar resultados de ambos tipos de tareas
        for future in as_completed(future_to_dep):
            data_dep_list.append(future.result())

    print("\n" * 10)
    data = pd.concat(data_dep_list, ignore_index=True).drop_duplicates()
    data["scraping_date"] = today
    # data = data.rename(columns=columns)
    data = clean_jobs_data(data)
    data.to_csv(data_path, index=False)
    # data["day_scrapper"] = today_sp

    #########
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

    end = time() - start
    day_data.to_csv(PATH_LOG, index=False)
    run_git_commands(end)

    # upload to supabase
    upload_and_drop_data(data)

    print(f"Tiempo de ejecución: {end } segundos")
