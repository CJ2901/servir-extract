from .types_src import *
from selenium.webdriver.common.by import By
import pandas as pd


def get_text(driver: WebDriver, xpath):
    text = driver.find_element(By.XPATH, xpath).text
    return text


def get_n_positions(driver: WebDriver, clasname="cuadro-vacantes"):
    try:

        n_position = len(driver.find_elements(By.CLASS_NAME, clasname))
        return n_position
    except:
        return 0


DETAILS_ATR = {
    "location": 1,
    "num_conv": 2,
    "n_vac": 3,
    "payment": 4,
    "begin_date": 5,
    "end_date": 6,
}

INFO = ["position", "institution"] + list(DETAILS_ATR.keys())  # columns


def xpath_box_details(
    n_box: int,
    details: str,
    prefix: str = '//*[@id="frmLstOfertsLabo"]/div/div/div[9]/div/div[{}]',
    sufix: str = "/div[2]/div[3]/div[{}]/div/span[2]",
):
    n_detail = DETAILS_ATR[details]
    loc_box = prefix.format(n_box)
    loc_detail = sufix.format(n_detail)
    return loc_box + loc_detail


def xpath_box_pos_inst(n_box: int):
    prefix = '//*[@id="frmLstOfertsLabo"]/div/div/div[9]/div/'

    position = prefix + f"div[{n_box}]/div[1]/div/label"
    institution = prefix + f"div[{n_box}]/div[2]/div[2]/span/b"
    return [position, institution]


def xpath_box(n_box):
    main_info = xpath_box_pos_inst(n_box)
    details_info = list(DETAILS_ATR.keys())
    details = [xpath_box_details(n_box, detail) for detail in details_info]
    return main_info + details


def get_info_box(driver: WebDriver, n_box):
    paths = xpath_box(n_box)
    ref_dict = {}

    for info, path in zip(INFO, paths):
        ref_dict[info] = get_text(driver, path)
    return ref_dict


def get_info_page(driver: WebDriver, n_total):
    actual_data = [get_info_box(driver, int(n) + 1) for n in range(n_total)]
    return pd.DataFrame(actual_data)
