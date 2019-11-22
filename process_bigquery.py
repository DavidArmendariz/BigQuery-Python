from google.cloud import bigquery
import os
import pandas as pd

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/david/Desktop/BigQuery-Python/cred_bq.json"
client = bigquery.Client()

sql = (
    "SELECT * FROM `zwippe-kpi-258416.netzwippe.conexiones` "
)

df = client.query(sql).to_dataframe()

df["Fecha_Ultima"] = pd.to_datetime(df["Fecha_Ultima"], format='%Y-%m-%d')
df["Dia"] = df["Fecha_Ultima"].dt.day_name(locale='es_EC.utf8')

print(df.columns.values)

# Comportamiento
visits_by_SSID = df.groupby(["Visitas", "SSID"])["Visitas"].sum()
visits_by_gender = df.groupby(["Visitas", "Genero"])["Visitas"].sum()
visits_by_platform = df.groupby(["Visitas", "Plataforma"])["Visitas"].sum()
visits_by_state = df.groupby(["Visitas", "Estado"])["Visitas"].sum()
visits_by_browser = df.groupby(["Visitas", "Navegador"])["Visitas"].sum()
visits_by_connection = df.groupby(["Visitas", "Conexion"])["Visitas"].sum()
Behavior = [visits_by_browser, visits_by_connection, visits_by_gender, visits_by_platform, visits_by_SSID,
            visits_by_state]

# Frecuencia
ts_by_browser = df.groupby(["Fecha_Ultima", "Navegador"])["Navegador"].count()
ts_by_connection = df.groupby(["Fecha_Ultima", "Conexion"])["Conexion"].count()
ts_by_gender = df.groupby(["Fecha_Ultima", "Genero"])["Genero"].count()
ts_by_SSID = df.groupby(["Fecha_Ultima", "SSID"])["SSID"].count()
Frequency = [ts_by_browser, ts_by_connection, ts_by_gender, ts_by_SSID]

# Resumen
ts_by_visits = df.groupby(["Fecha_Ultima"])["Visitas"].sum()
gender_accum = df.groupby(["Genero"])["Visitas"].sum()
age_accum = df.groupby(["Edad"])["Visitas"].sum()
days_accum = df.groupby(["Dia"])["Visitas"].sum()
status_accum = df.groupby(["Estado"])["Visitas"].sum()
connection_accum = df.groupby(["Conexion"])["Visitas"].sum()
Overview = [ts_by_visits, gender_accum, age_accum, days_accum, status_accum, connection_accum]


def process_frequency(multiindex_df):
    new = pd.MultiIndex.from_product(multiindex_df.index.levels, names=multiindex_df.index.names)
    multiindex_df = multiindex_df.reindex(new, fill_value=0)
    dates = multiindex_df.index.unique(0).to_list()
    data = []
    for d in dates:
        temp = {"day": d.day, "month": d.month, "year": d.year}
        temp.update(multiindex_df[d].to_dict())
        data.append(temp)
    return data


def process_behaviour(multiindex_df):
    new = pd.MultiIndex.from_product(multiindex_df.index.levels, names=multiindex_df.index.names)
    multiindex_df = multiindex_df.reindex(new, fill_value=0)
    visits = multiindex_df.index.unique(0).to_list()
    data = []
    for v in visits:
        temp = {"visits": v}
        temp.update(multiindex_df[v].to_dict())
        data.append(temp)
    return data
