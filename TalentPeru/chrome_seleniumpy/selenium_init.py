from selenium import webdriver

import time, pandas as pd


import pytz
import datetime

gmt5 = pytz.timezone("Etc/GMT+5")
today = datetime.datetime.now(gmt5).strftime("%d-%m-%Y")
import os

from .data_in_page import get_info_page, get_info_box, get_n_positions
from .filters_page import filter_region
from .utils import query_success
from .navigation_pages import navigate_to, page_num


def scrapper(options=None, n_reg=[1], github=False):

    if options is not None:
        driver = webdriver.Chrome(options=options)
    else:
        driver = webdriver.Chrome()
    driver.get(
        "https://app.servir.gob.pe/DifusionOfertasExterno/faces/consultas/ofertas_laborales.xhtml"
    )
    print("Complete loading")
    driver.minimize_window()

    for reg in n_reg:
        data, reg_name, n_text = find_in_region(driver, reg)
        save_data(data, reg_name, n_text)
    driver.quit()


def find_in_region(driver, n_reg):
    location = filter_region(driver, n_reg)
    num_works = query_success(driver)

    total_positions_in_page = get_n_positions(driver)
    begin_page, total_pages = page_num(driver)

    data = get_info_page(driver, total_positions_in_page)

    while begin_page < total_pages:
        begin_page, total_pages = navigate_to(driver)
        total_positions_in_page = get_n_positions(driver)
        result_page = get_info_page(driver, total_positions_in_page)
        data = pd.concat([data, result_page], ignore_index=True)
        print(begin_page, total_pages)

    return data, location, num_works


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

    # actualizar
    # departamento, ultima fecha, path_location


import re
import unicodedata


def limpiar_texto(texto):
    texto = (
        unicodedata.normalize("NFKD", texto).encode("ascii", "ignore").decode("utf-8")
    )
    texto = re.sub(r"\s+", " ", texto.strip())
    texto_limpio = re.sub(r"[^a-zA-Z0-9 ]", "", texto)
    texto_limpio = texto_limpio.replace(" ", "_")
    return texto_limpio.lower()


def extract_njobs(text):
    numero = re.search(r"\d+", text)

    if numero:
        return int(numero.group(0))
    else:
        return None
