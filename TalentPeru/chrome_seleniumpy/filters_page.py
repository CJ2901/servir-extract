# aplicacion de los filtros

from .types_src import *
import time


# location dropdown (ubicacion)

region_filter_dropdown = '//*[@id="frmLstOfertsLabo:cboDep"]/div[3]/span'
# //*[@id="frmLstOfertsLabo:cboDep"]/div[3]/span

# //*[@id="frmLstModFormativas:cboDep"]/div[3]/span
ID_index = "frmLstOfertsLabo:cboDep_{}"
apply_filter = '//*[@id="frmLstOfertsLabo:j_idt42"]/span'
# #


def verify_element(xpath: str, driver: WebDriver):
    find = driver.find_elements(By.XPATH, xpath)
    return find, len(find) > 0


def success(driver: WebDriver, xpath: str, attemps=15):
    find, popup = verify_element(xpath, driver)
    n = 1
    while not popup | n <= attemps:
        time.sleep(1)
        print("loading", popup)
        find, popup = verify_element(xpath, driver)


# //*[@id="frmLstOfertsLabo:cboDep_1"]
def filter_region(driver: WebDriver, n_list: int):
    """
    Seleccciona por el id la ubicacion y aplica el filtro
    """

    # seleccionar filtro
    # complete_web(driver, region_filter_dropdown, 100)
    wait = WebDriverWait(driver, 10)
    element = wait.until(EC.element_to_be_clickable((By.XPATH, region_filter_dropdown)))

    driver.find_element(By.XPATH, region_filter_dropdown).click()
    time.sleep(1)
    item_drop = driver.find_element(By.ID, ID_index.format(n_list))
    # print(ID_index.format(n_list))
    location_main = item_drop.text
    print(location_main)
    item_drop.click()

    # aplicar filtro
    driver.find_element(By.XPATH, apply_filter).click()
    return location_main
