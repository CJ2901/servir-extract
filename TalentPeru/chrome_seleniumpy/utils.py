from .types_src import *
import time

# pop up para cuando la pagina se cargo correctamente
loading_success = '//*[@id="frmLstOfertsLabo:mensaje_container"]/div/div/div[2]/p'
loading_page = "j_idt85"


def query_success(driver: WebDriver):
    """
    Verifica que si despues de aplicar los filtros se
    carga los elementos correctos y da 1 segundo adicional
    para poder cargar
    """
    time.sleep(3)

    def success():
        find = driver.find_elements(By.XPATH, loading_success)
        return find, len(find) > 0  # encontro el elemento

    find, popup = success()
    while not popup:
        time.sleep(1)
        print("loading", popup)
        find, popup = success()
    time.sleep(1)
    return find[0].text


def change_page_loading(driver: WebDriver):
    def get_loading():
        div1 = driver.find_element(By.ID, loading_page)
        style = div1.get_attribute("style")
        loading = "display: block" in style
        return loading

    loading = get_loading()
    while loading:
        time.sleep(0.1)
        print(".", end="")
        loading = get_loading()
    time.sleep(0.5)
