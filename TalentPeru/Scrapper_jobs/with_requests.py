import requests
import warnings
import pandas as pd
from rich import print
from tqdm import tqdm
from bs4 import BeautifulSoup as bsoup
from .utils import (
    limpiar_espacios,
    total_numbers,
    goto_first_dep_page_payload,
    goto_next_page_payload,
    goto_last_page_payload,
    goto_prev_page_payload,
)
from .global_env import URL, HEADERS, PAYLOAD

warnings.filterwarnings("ignore")


def get_view_state(soup):
    view_state_input = soup.find("input", {"name": "javax.faces.ViewState"})
    view_state_value = view_state_input["value"]

    return view_state_value


def first_session(url=URL, head=HEADERS):

    session = requests.Session()

    fp_request = session.get(url, headers=head)

    fp_page_soup = bsoup(fp_request.content, features="lxml")
    view_state_value = get_view_state(fp_page_soup)
    return session, fp_page_soup, view_state_value


def goto_page(data: dict, session: requests.Session):
    result_page = session.post(url=URL, headers=HEADERS, data=data).content
    return result_page


def souper(content_html):
    soup_html = bsoup(content_html, features="lxml")
    return soup_html


def get_label_page(soup_html):

    page_n = soup_html.find("label", class_="control-label btn-paginator-cnt").text
    return page_n


def find_jobs(page: bsoup):
    jobs_info = page.find_all("div", class_="cuadro-vacantes")
    return jobs_info


def get_info_jobs(job_n):
    """
    Obtienen la informacion de los trabajos
    """
    position = job_n.find("div", class_="titulo-vacante").get_text(strip=True).replace("\n", "")

    spans_cuadro = job_n.find_all("span", class_="detalle-sp")
    # print(psan)
    info = [limpiar_espacios(span.get_text(strip=True)) for span in spans_cuadro]
    return [position] + info


def data_in_page_f(session, payload):
    content_html = goto_page(session=session, data=payload)

    html_page = souper(content_html)
    numbers = get_label_page(html_page)
    _, total = total_numbers(numbers)
    data_in_page = convert_in_df(html_page)
    return data_in_page, total


def get_data_next_page(session, payload, last_number_page=None):
    data = []

    html, total_pages = data_in_page_f()
    if last_number_page is None:
        last_number_page = total_pages
    print(last_number_page)
    for num_page in tqdm(range(last_number_page - 1)):

        data_in_page, _ = data_in_page_f()
        data.append(data_in_page)
    return pd.concat(data)


def convert_in_df(html_page) -> pd.DataFrame:
    jobs = find_jobs(html_page)  # all jobs
    info_jobs = []

    for job in jobs:
        info_jobs.append(get_info_jobs(job))

    data = pd.DataFrame(
        info_jobs,
        columns=[
            "position",
            "institution",
            "ubication",
            "num_conv",
            "n_vac",
            "wage",
            "begin_date",
            "end_date",
        ],
    )

    return data


# Logica: Primera pagina, siguiente pagina, ultima pagina (pequenos datos)
# logica: primera pagina, (siguiente pagina -> hasta encontrarse, ultima pagina -> previa pagina hasta encontrarse)


def data_souper(page_content):
    html_ = souper(page_content)
    page_n = get_label_page(html_)
    actual, total = total_numbers(page_n)
    data = convert_in_df(html_)
    return data, actual, total


def go_first_page(view_state_value, dep, session) -> pd.DataFrame:
    first_page_payload = goto_first_dep_page_payload(view_state_value, dep)
    first_page_reg = goto_page(first_page_payload, session)
    return data_souper(first_page_reg)


def go_last_page(view_state_value, dep, session) -> pd.DataFrame:
    last_page_payload = goto_last_page_payload(view_state_value, dep)
    last_page_reg = goto_page(data=last_page_payload, session=session)
    return data_souper(last_page_reg)


def go_next_page(view_state_value, dep, session) -> pd.DataFrame:
    next_page_payload = goto_next_page_payload(view_state_value, dep)
    next_page_reg = goto_page(data=next_page_payload, session=session)
    return data_souper(next_page_reg)


def go_prev_page(view_state_value, dep, session) -> pd.DataFrame:
    prev_page_payload = goto_prev_page_payload(view_state_value, dep)
    prev_page_reg = goto_page(data=prev_page_payload, session=session)
    return data_souper(prev_page_reg)


def left_to_rigth(view_state_value, dep, session, right=False):
    # Scrappea los datos desde la primera pagina, siguiente pagian ...
    data, n, total = go_first_page(view_state_value, dep, session)

    data_list = [data]
    if right:
        total = int(total / 2) + 1

    for n in tqdm(range(total - 1)):
        try:
            data, n_i, _ = go_next_page(view_state_value, dep, session)
            data_list.append(data)
        except:
            pass

    return pd.concat(data_list, ignore_index=True)


def right_to_left(view_state_value, dep, session, left=True):
    # Scrapea los datos desde la primera pagina, va a la ultima pagina, y de ahi previa pagina
    data, n, total = go_first_page(view_state_value, dep, session)

    data, n, total = go_last_page(view_state_value, dep, session)
    data_list = [data]

    if left:
        total = int(total / 2) + 2

    for n in tqdm(range(total - 2)):
        data, n_i, _ = go_prev_page(view_state_value, dep, session)
        data_list.append(data)

    return pd.concat(data_list, ignore_index=True)
