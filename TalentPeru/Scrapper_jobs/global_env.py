# global_env.py
from rich import print

URL = "https://app.servir.gob.pe/DifusionOfertasExterno/faces/consultas/ofertas_laborales.xhtml"
# HEADERS = {
#     "User-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.129 Safari/537.36",
#     "X-Requested-With": "XMLHttpRequest",
# }
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Host": "app.servir.gob.pe",
    "Sec-Fetch-User": "?1",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Dest": "document",
}
PAYLOAD = {
    "javax.faces.partial.ajax": "true",
    "javax.faces.source": "frmLstOfertsLabo:j_idt56",
    "javax.faces.partial.execute": "@all",
    "javax.faces.partial.render": "frmLstOfertsLabo:mensaje frmLstOfertsLabo",
    "frmLstOfertsLabo:j_idt56": "frmLstOfertsLabo:j_idt56",
    "frmLstOfertsLabo": "frmLstOfertsLabo",
    "frmLstOfertsLabo:modalidadAcceso": "03",
    "frmLstOfertsLabo:txtPerfil": "",
    "frmLstOfertsLabo:cboDep_focus": "",
    "frmLstOfertsLabo:cboDep_input": 0,
    "frmLstOfertsLabo:txtPuesto": "",
    "frmLstOfertsLabo:autocompletar_input": "",
    "frmLstOfertsLabo:autocompletar_hinput": "",
    "frmLstOfertsLabo:txtNroConv": "",
}


share_payload = {
    "javax.faces.partial.ajax": "true",
    "javax.faces.partial.execute": "@all",
    "javax.faces.partial.render": "frmLstOfertsLabo:mensaje frmLstOfertsLabo",
    "frmLstOfertsLabo": "frmLstOfertsLabo",
    "frmLstOfertsLabo:modalidadAcceso": "03",
    "frmLstOfertsLabo:txtPerfil": "",
    "frmLstOfertsLabo:cboDep_focus": "",
    "frmLstOfertsLabo:txtPuesto": "",
    "frmLstOfertsLabo:autocompletar_input": "",
    "frmLstOfertsLabo:autocompletar_hinput": "",
    "frmLstOfertsLabo:txtNroConv": "",
}

# falta el view State
# lima 15

first_page = {
    "javax.faces.source": "frmLstOfertsLabo:j_idt42",
    "frmLstOfertsLabo:j_idt42": "frmLstOfertsLabo:j_idt42",
}
next_page = {
    "javax.faces.source": "frmLstOfertsLabo:j_idt56",
    "frmLstOfertsLabo:j_idt56": "frmLstOfertsLabo:j_idt56",
}

last_page = {
    "javax.faces.source": "frmLstOfertsLabo:j_idt57",
    "frmLstOfertsLabo:j_idt57": "frmLstOfertsLabo:j_idt57",
}
prev_page = {
    "javax.faces.source": "frmLstOfertsLabo:j_idt54",
    "frmLstOfertsLabo:j_idt54": "frmLstOfertsLabo:j_idt54",
}


dep = "01"
view_state_value = "12345678901234567890123456789012"

# print(goto_first_page_payload(view_state_value, dep))
# print(goto_next_page_payload(view_state_value, dep))
# print(goto_last_page_payload(view_state_value, dep))
# print(goto_prev_page_payload(view_state_value, dep))

depa_value = {
    "01": "AMAZONAS",
    "02": "ANCASH",
    "03": "APURIMAC",
    "04": "AREQUIPA",
    "05": "AYACUCHO",
    "06": "CAJAMARCA",
    "07": "CALLAO",
    "08": "CUSCO",
    "09": "HUANCAVELICA",
    "10": "HUANUCO",
    "11": "ICA",
    "12": "JUNIN",
    "13": "LA LIBERTAD",
    "14": "LAMBAYEQUE",
    "15": "LIMA",
    "16": "LORETO",
    "17": "MADRE DE DIOS",
    "18": "MOQUEGUA",
    "19": "PASCO",
    "20": "PIURA",
    "21": "PUNO",
    "22": "SAN MARTIN",
    "23": "TACNA",
    "24": "TUMBES",
    "25": "UCAYALI",
}
