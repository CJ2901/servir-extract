import re
import uuid
from bs4 import BeautifulSoup
import requests
import math
import pandas as pd, numpy as np
import warnings, tqdm

from .global_env import URL, HEADERS, depa_value
from .utils import (
    goto_dep_payload,
    goto_first_dep_page_payload,
    goto_last_page_payload,
    goto_next_page_payload,
    goto_prev_page_payload,
)

warnings.filterwarnings("ignore")

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


shared_columns = [
    "public_institution",
    "ubication",
    "job_posting_number",
    "vacancies",
    "salary",
    "start_publication_date",
    "end_publication_date",
    "job_title",
]
# cards_columns = shared_columns + ["ubication"]


def remove_extra_spaces(text):
    """Removes multiple spaces from a string."""
    return re.sub(r"\s+", " ", text).strip()


def paginator(texto: str):
    # Expresión regular para encontrar secuencias de dígitos
    numeros = re.findall(r"\d+", texto)

    return [int(num) for num in numeros]


class JobScrapper:
    def __init__(
        self,
        dep="01",
        url=URL,
        headers=HEADERS,
    ):
        self.url = url
        self.headers = headers
        self.dep = dep
        # Obtenenmos la session y el valor de view_value
        self.first_session()

        # vamos a la primera pagina del departamento
        self.soup = self.go_first_page()
        # obtenemos el total de paginas existentes
        self.get_label_page()

        self.scrapper_fn = JobScraperDetails(
            view_value=self.view_state_value, session=self.session
        )
        self.data = []
        self.card_data = []

    # @staticmethod
    def scrapper_cards(self, html):
        jobs_info = html.find_all("div", class_="cuadro-vacantes")

        data_cards = []
        for job in jobs_info:
            position = job.find("div", class_="titulo-vacante").get_text(strip=True)
            card_details = job.find_all("span", class_="detalle-sp")
            info = [
                remove_extra_spaces(card_detail.get_text(strip=True))
                for card_detail in card_details
            ]
            data_cards.append(info + [position])
        # self.data_cards
        self.card_data.append(pd.DataFrame(data_cards))
        return self

    def scrapper_page(self, n=10) -> list:

        try:
            # insertar ubicacion por parametro
            detail_scrapper = self.scrapper_fn.get_details_data_in_page(n)

            self.data.append(detail_scrapper)
            return detail_scrapper
        except:
            pass

    def scrapper_seq(self, iteration_total, change_page, side=None):
        dep = self.dep
        name_depa = depa_value[dep].title()
        name_description = f"Departamento de {name_depa} - {dep} "

        if side is not None:
            name_description = f"Departamento de {name_depa} - {side}"
        if dep == "15":
            colour = "blue"
        else:
            colour = None

        for _ in tqdm.tqdm(
            range(iteration_total), desc=name_description, colour=colour
        ):
            page_content_jobs = change_page()
            self.scrapper_cards(page_content_jobs)

            jobs = page_content_jobs.find_all("div", class_="cuadro-vacantes")
            n_jobs = len(jobs)
            # print(n_jobs)
            self.scrapper_page(n_jobs)

    @staticmethod
    def get_metadata_data(html_soup):
        ubication_spans = html_soup.find_all("span", text="Ubicación:")
        locations = [
            ubication.find_next("span", class_="detalle-sp").get_text(strip=True)
            for ubication in ubication_spans
        ]
        locations = [remove_extra_spaces(loc) for loc in locations]
        return locations

    def get_data(self):
        data_details = pd.concat(self.data, ignore_index=True)
        data_details = data_details.rename(columns=columns)
        card_data = pd.concat(self.card_data, ignore_index=True)
        card_data.columns = shared_columns
        title = ["job_posting_number", "public_institution"]
        for t in title:
            card_data[t] = card_data[t].str.title().str.strip()
        try:
            data_details = pd.merge(data_details, card_data, how="left")
        except:
            try:
                data_details["ubication"] = card_data["ubication"]
            except:
                pass
        data_details = data_details.replace({np.nan: None})
        return data_details

    def scrapper_sequential(self):
        total_pages = self.total_pages
        self.scrapper_cards(self.soup)
        self.scrapper_page()  # primera pagian
        n_discount = 1
        # print(total_pages)

        iteration_total = total_pages - n_discount
        change_page = self.go_next_page
        self.scrapper_seq(iteration_total, change_page)
        return self.get_data()

    def scrapper_both_left(self):
        self.scrapper_page()
        change_page = self.go_next_page

        total_pages = self.total_pages
        n_discount = 2
        iteration_total = math.floor((total_pages - n_discount) / 2)

        self.scrapper_seq(iteration_total, change_page, side="izquierda a derecha")
        return self.get_data()

    def scrapper_both_right(self):
        page_content_jobs = self.go_last_page()
        self.scrapper_cards(page_content_jobs)

        jobs = page_content_jobs.find_all("div", class_="cuadro-vacantes")
        n_jobs = len(jobs)
        # ubication = self.get_ubication(page_content_jobs)
        self.scrapper_page(n_jobs)
        change_page = self.go_prev_page

        total_pages = self.total_pages
        n_discount = 2
        iteration_total = math.ceil((total_pages - n_discount) / 2)

        self.scrapper_seq(iteration_total, change_page, side="derecha a izquierda")
        return self.get_data()

    # @staticmethod
    def get_label_page(self):

        page_n = self.soup.find("label", class_="control-label btn-paginator-cnt").text
        actual_page, last_page = paginator(page_n)
        self.total_pages = last_page
        return page_n

    @staticmethod
    def get_view_state(soup):
        """Obtiene el valor del view state del HTML."""
        view_state_input = soup.find("input", {"name": "javax.faces.ViewState"})
        view_state_value = view_state_input["value"]
        return view_state_value

    def first_session(self):
        session = requests.Session()
        fp_requests = session.get(self.url, headers=self.headers)
        # Es la pagina que se muestra al cargar no es relevante por el momento
        fp_soup = BeautifulSoup(fp_requests.content, features="lxml")
        view_state_vale = self.get_view_state(fp_soup)
        self.session = session
        self.view_state_value = view_state_vale
        self.fpsoup = fp_soup
        return self

    def goto_page(self, data: dict):
        """Realiza una petición POST para navegar entre páginas. Retorna un Html"""
        result_page = self.session.post(
            url=self.url, headers=self.headers, data=data
        ).content
        return result_page

    def go_first_page(self):
        first_page_payload = goto_first_dep_page_payload(
            self.view_state_value, self.dep
        )
        first_page_reg = self.goto_page(first_page_payload)
        return BeautifulSoup(first_page_reg, features="lxml")

    def go_next_page(self):
        next_page_payload = goto_next_page_payload(self.view_state_value, self.dep)
        next_page_reg = self.goto_page(next_page_payload)
        return BeautifulSoup(next_page_reg)

    def go_last_page(self):
        last_page_payload = goto_last_page_payload(self.view_state_value, self.dep)
        last_page_reg = self.goto_page(last_page_payload)
        return BeautifulSoup(last_page_reg)

    def go_prev_page(self):
        prev_page_payload = goto_prev_page_payload(self.view_state_value, self.dep)
        prev_page_reg = self.goto_page(prev_page_payload)
        return BeautifulSoup(prev_page_reg)

    # def


import pandas as pd


class JobScraperDetails:
    # No depende del html presentado en la carga, si no se basa en como funciona
    # la pagina principal -> hace un post a la pagina principal con el id que se requiere
    # luego hace un requests del detail
    def __init__(self, view_value, session):
        self.view_value = view_value
        self.session = session

    def get_n_job_description(self, job_n):
        headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "es,en;q=0.9,en-US;q=0.8",
            "cache-control": "no-cache",
            "content-type": "application/x-www-form-urlencoded",
            "pragma": "no-cache",
            "sec-ch-ua": '"Not)A;Brand";v="99", "Opera";v="113", "Chromium";v="127"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "same-origin",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
        }
        payload = {
            "frmLstOfertsLabo": "frmLstOfertsLabo",
            "frmLstOfertsLabo:modalidadAcceso": "03",
            "frmLstOfertsLabo:txtPerfil": "",
            "frmLstOfertsLabo:cboDep_focus": "",
            "frmLstOfertsLabo:cboDep_input": "15",
            "frmLstOfertsLabo:txtPuesto": "",
            "frmLstOfertsLabo:autocompletar_input": "",
            "frmLstOfertsLabo:autocompletar_hinput": "",
            "frmLstOfertsLabo:txtNroConv": "",
            f"frmLstOfertsLabo:idPnlRepeatPuestos:{job_n}:j_idt71": "",
            "javax.faces.ViewState": self.view_value,
        }
        url_post = "https://app.servir.gob.pe/DifusionOfertasExterno/faces/consultas/ofertas_laborales.xhtml"
        url_get = "https://app.servir.gob.pe/DifusionOfertasExterno/faces/consultas/detalle_ofertas_laborales.xhtml"

        # Realiza las solicitudes POST y GET
        self.session.post(url_post, headers=headers, data=payload, timeout=1)
        detail_i = self.session.get(url_get, headers=headers, timeout=1)

        return detail_i

    def get_details_data(self, html_soup_n):
        details = html_soup_n.find_all("div", class_="seccion-detalle")
        title_details = details[0]  # position, institution
        job_details = details[1]  # details
        job_details_dtl = job_details.find_all(
            "span", class_="sub-titulo"
        )  # Span details

        position = title_details.find("span", class_="sp-aviso0").get_text(strip=True)
        institution = title_details.find("span", class_="sp-aviso").get_text(strip=True)

        # Requisitos específicos
        job_req = job_details.find("p", class_="sub-titulo").find_all_next(
            "span", class_="sub-titulo-2"
        )

        requerimientos_job = {}
        for jr in job_req:
            key_req = self.remove_extra_spaces(jr.text.strip()).title()
            detail_req = jr.find_next("span").get_text(strip=True)
            if len(detail_req) > 0:
                requerimientos_job[key_req] = detail_req

        job_more_details = {}
        for jb in job_details_dtl:
            name = self.remove_extra_spaces(jb.text).title()
            value_html = jb.find_next("span")
            if name.lower() == "detalle:":
                # Obtener URL de la convocatoria
                name = "url_convocatoria"
                value = value_html.find("a", href=True)["href"]
                if value == "#":
                    value = value_html.get_text(strip=True).lower()
            else:
                value = self.remove_extra_spaces(
                    value_html.get_text(strip=True)
                ).title()
            job_more_details[name] = value

        job_more_details.update(requerimientos_job)
        job_more_details["position"] = position
        job_more_details["institution"] = institution.title()
        job_more_details["uuid"] = str(
            uuid.uuid5(uuid.NAMESPACE_DNS, position + institution.title())
        )

        return job_more_details

    def get_details_data_in_page(self, n) -> list:
        data_in_page = []
        for i in range(n):
            soup_i = BeautifulSoup(self.get_n_job_description(i).content, "html.parser")
            result_i = self.get_details_data(soup_i)
            data_in_page.append(result_i)
        data = pd.DataFrame(data_in_page)

        return data

    def remove_extra_spaces(self, text):
        """Función auxiliar para limpiar espacios extra."""
        return " ".join(text.split())
