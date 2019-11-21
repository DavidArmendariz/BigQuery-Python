from google.cloud import bigquery
import os
import pandas as pd

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/david/Desktop/BigQuery-Python/cred.json"
client = bigquery.Client()

sql = (
    "SELECT * FROM `zwippe-kpi-258416.netzwippe.conexiones` "
)

df = client.query(sql).to_dataframe()

print(df.columns.values)

visits_by_SSID = df.groupby(["Visitas", "SSID"])["Visitas"].sum()
visits_by_gender = df.groupby(["Visitas", "Genero"])["Visitas"].sum()
visits_by_platform = df.groupby(["Visitas", "Plataforma"])["Visitas"].sum()
visits_by_state = df.groupby(["Visitas", "Estado"])["Visitas"].sum()
visits_by_browser = df.groupby(["Visitas", "Navegador"])["Visitas"].sum()
visits_by_connection = df.groupby(["Visitas", "Conexion"])["Visitas"].sum()

ts_by_browser = df.groupby(["Fecha_Ultima", "Navegador"])["Navegador"].count()
ts_by_connection = df.groupby(["Fecha_Ultima", "Conexion"])["Conexion"].count()
ts_by_gender = df.groupby(["Fecha_Ultima", "Genero"])["Genero"].count()
ts_by_SSID = df.groupby(["Fecha_Ultima", "SSID"])["SSID"].count()

ts_by_visits =

# To get the index of the first level we do:
# list(set(visits_by_connection.index.get_level_values(0).tolist()))
