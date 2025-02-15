# utils.py

from .global_env import share_payload, first_page, next_page, last_page, prev_page
import re


def job_wage_float(wage: str):
    # 'S/. 1,950.00' -> 1950.00
    wage = wage.replace(",", "").replace("S/.", "").strip()
    return float(wage)


def limpiar_espacios(texto):
    # Eliminar tabulaciones y reemplazarlas con un solo espacio
    texto = re.sub(r"\s{2,}", " ", texto)
    texto = re.sub(r"\t+", " ", texto)
    texto = re.sub(r"\n+", " ", texto)
    # Eliminar múltiples espacios (más de 2) y reemplazarlos con un solo espacio
    # Eliminar espacios al principio y al final
    return texto.strip()


def total_numbers(texto: str):
    # Expresión regular para encontrar secuencias de dígitos
    numeros = re.findall(r"\d+", texto)
    # Convertir los números encontrados a enteros
    return [int(num) for num in numeros]


def goto_dep_payload(view_state_value, dep_search):
    shared_payload_reg = share_payload.copy()

    shared_payload_reg.update(
        {
            "javax.faces.ViewState": view_state_value,
            "frmLstOfertsLabo:cboDep_input": dep_search,
        }
    )

    return shared_payload_reg


def goto_first_dep_page_payload(view_state_value, dep_search):
    payload = goto_dep_payload(view_state_value, dep_search)
    first_page_payload = payload.copy()
    first_page_payload.update(first_page)
    return first_page_payload


def goto_next_page_payload(view_state_value, dep_search):
    dep_payload = goto_dep_payload(
        view_state_value=view_state_value, dep_search=dep_search
    )
    dep_payload.update(next_page)
    return dep_payload


def goto_last_page_payload(view_state_value, dep_search):
    dep_payload = goto_dep_payload(
        view_state_value=view_state_value, dep_search=dep_search
    )

    dep_payload.update(last_page)
    return dep_payload


def goto_prev_page_payload(view_state_value, dep_search):
    dep_payload = goto_dep_payload(
        view_state_value=view_state_value, dep_search=dep_search
    )
    dep_payload.update(prev_page)
    return dep_payload
