Scrapeando las oportunidades laborales de https://app.servir.gob.pe/DifusionOfertasExterno/faces/consultas/ofertas_laborales.xhtml servir

# Uso

## Scrapear datos

```
pip install -r requirements.txt
python main.py
```

## Api

```
uvicorn app:app --reload
```

### Metodos

```python
URL = "https://talento-peru-jobs-kibob9qxx-tjhons-projects.vercel.app/"
# ultimos datos registrados
/jobs/last
# Registro de datos de scrapeos
/jobs/logs
# Datos por fecha
jobs/history/02-10-2024
```


# TODO:

- [ ] Scrappear datos
  - [x] Datos Generales
  - [ ] Instituciones del estado -> https://www.datosabiertos.gob.pe/sites/default/files/ds_lista_entidades_4.csv
  - [ ] Detalles
    - [ ] Usar las paginas de convocatoria de cada institucion - (ideal)
- [ ] Api: Fast API
  - [ ] Documentacion
- [ ] Metricas - Analisis de datos
- [ ] DataFrame format
- [ ] Web App
