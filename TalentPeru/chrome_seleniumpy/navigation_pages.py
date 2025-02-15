# Para movernos entre paginas


from .types_src import *
from .utils import change_page_loading
import time, re


TYPE_PAGES = {
    "last": '//*[@id="frmLstOfertsLabo:j_idt57"]/span[2]',
    "prev": '//*[@id="frmLstOfertsLabo:j_idt54"]/span[2]',
    "next": '//*[@id="frmLstOfertsLabo:j_idt56"]/span[2]',
}
PAGE_ACTUAL = '//*[@id="frmLstOfertsLabo"]/div/div/div[8]/div[2]/label'


def page_num(driver: WebDriver, regex=r"\d+"):
    actual_page_text = driver.find_element(By.XPATH, PAGE_ACTUAL).text
    page_info = re.findall(regex, actual_page_text)
    return [int(page) for page in page_info]


def navigate_to(driver: WebDriver, to="next", wait=4):
    """
    navega entre paginas y retorna la pagina actual y la pagina final
    """
    xpath = TYPE_PAGES[to]
    driver.find_element(By.XPATH, xpath).click()
    change_page_loading(driver)
    return page_num(driver)
