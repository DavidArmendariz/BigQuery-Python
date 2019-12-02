from google.cloud import bigquery
import os
import pandas as pd
import numpy as np
from datetime import datetime
import json

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/david/Desktop/BigQuery-Python/cred_bq.json"
client = bigquery.Client()

sql = (
    "SELECT * FROM `zwippe-kpi-258416.netzwippe.conexiones` "
)

df = client.query(sql).to_dataframe()

df["Fecha_Ultima"] = pd.to_datetime(df["Fecha_Ultima"], format='%Y-%m-%d')
df["Dia"] = df["Fecha_Ultima"].dt.day_name(locale='es_EC.utf8')

# Comportamiento
visits_by_SSID = df.groupby(["Fecha_Ultima", "Visitas", "SSID"])["Visitas"].sum()
visits_by_gender = df.groupby(["Fecha_Ultima", "Visitas", "Genero"])["Visitas"].sum()
visits_by_platform = df.groupby(["Fecha_Ultima", "Visitas", "Plataforma"])["Visitas"].sum()
visits_by_state = df.groupby(["Fecha_Ultima", "Visitas", "Estado"])["Visitas"].sum()
visits_by_browser = df.groupby(["Fecha_Ultima", "Visitas", "Navegador"])["Visitas"].sum()
visits_by_connection = df.groupby(["Fecha_Ultima", "Visitas", "Conexion"])["Visitas"].sum()
visits_by_Age = df.groupby(["Fecha_Ultima", "Visitas", "Edad"])["Visitas"].sum()
Behavior = [visits_by_browser, visits_by_connection, visits_by_gender, visits_by_platform, visits_by_SSID,
            visits_by_state, visits_by_Age]

# Frecuencia
ts_by_browser = df.groupby(["Fecha_Ultima", "Navegador"])["Navegador"].count()
ts_by_connection = df.groupby(["Fecha_Ultima", "Conexion"])["Conexion"].count()
ts_by_gender = df.groupby(["Fecha_Ultima", "Genero"])["Genero"].count()
ts_by_SSID = df.groupby(["Fecha_Ultima", "SSID"])["SSID"].count()
Frequency = [ts_by_browser, ts_by_connection, ts_by_gender, ts_by_SSID]

# Resumen
ts_by_visits = df.groupby(["Fecha_Ultima"])["Visitas"].sum()
gender_accum = df.groupby(["Fecha_Ultima", "Genero"])["Visitas"].sum()
age_accum = df.groupby(["Fecha_Ultima", "Edad"])["Visitas"].sum()
days_accum = df.groupby(["Fecha_Ultima", "Dia"])["Visitas"].sum()
status_accum = df.groupby(["Fecha_Ultima", "Estado"])["Visitas"].sum()
connection_accum = df.groupby(["Fecha_Ultima", "Conexion"])["Visitas"].sum()
Overview = [ts_by_visits, gender_accum, age_accum, days_accum, status_accum, connection_accum]


def process_frequency(multiindex_df):
    new = pd.MultiIndex.from_product(multiindex_df.index.levels, names=multiindex_df.index.names)
    multiindex_df = multiindex_df.reindex(new, fill_value=0)
    dates = multiindex_df.index.unique(0).to_list()
    data = []
    for d in dates:
        ts = (d - np.datetime64('1970-01-01T00:00:00Z')) / np.timedelta64(1, 's')
        temp = {"day": d.day, "month": d.month, "year": d.year, "date": datetime.utcfromtimestamp(ts)}
        temp.update(multiindex_df[d].to_dict())
        data.append(temp)
    return data


def process_behaviour(multiindex_df):
    new = pd.MultiIndex.from_product(multiindex_df.index.levels, names=multiindex_df.index.names)
    multiindex_df = multiindex_df.reindex(new, fill_value=0)
    dates = multiindex_df.index.unique(0).to_list()
    data = []
    for d in dates:
        temp = {"date": d}
        temp.update(multiindex_df[d].to_dict())
        data.append(temp)
    return data


def process_overview(df):
    dates = df.index.tolist()
    data = []
    for d in dates:
        ts = (d - np.datetime64('1970-01-01T00:00:00Z')) / np.timedelta64(1, 's')
        temp = {"day": d.day, "month": d.month, "year": d.year, "date": datetime.utcfromtimestamp(ts)}
        temp.update({"value": df[d].item()})
        data.append(temp)
    return data


def process_behaviour_comportamiento(multiindex_df):
    new = pd.MultiIndex.from_product(multiindex_df.index.levels, names=multiindex_df.index.names)
    multiindex_df = multiindex_df.reindex(new, fill_value=0)
    dates = multiindex_df.index.unique(0).to_list()
    visits = multiindex_df.index.unique(1).to_list()
    data = []
    for d in dates:
        temp = {"date": d}
        for v in visits:
            temp.update({str(v): json.loads(multiindex_df[d][v].to_json())})
        data.append(temp)
    return data
