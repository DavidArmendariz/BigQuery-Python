from google.cloud import bigquery
import os
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
import random

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "cred_bq.json"
client = bigquery.Client()

sql = (
    "SELECT * FROM `zwippe-kpi-258416.netzwippe.conexiones` "
)

df = client.query(sql).to_dataframe()
x = df["Genero"].count().item()

try:
    cred = credentials.Certificate("cred_firestore.json")
    firebase_admin.initialize_app(cred, {'databaseURL': 'https://zwippe-app-dev.firebaseio.com'})
    db = firestore.client()
    ref = db.collection('test').document('test')
    ref.update({
        'category' + str(random.randint(0, 100)): x
    })
except:
    print(x)
