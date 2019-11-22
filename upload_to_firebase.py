from process_bigquery import Behavior, Frequency, Overview, process_frequency, process_behaviour
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore

try:
    cred = credentials.Certificate("cred_firestore.json")
    firebase_admin.initialize_app(cred, {'databaseURL': 'https://zwippe-app-dev.firebaseio.com'})
    db = firestore.client()
except:
    pass

########################################################################################################################
########################################################################################################################
ref = db.collection('dashboard').document('malteria').collection('pages').document('mREBXDwkqgk329kHj4Lb')
ref.set({
    'category': 'Dashboard',
    'name': 'Freq',
    'path': '/Freq',
    'type': 'FREQUENCY ANALYTICS',
    'icon': 'show_chart'
})

for i in range(len(Frequency)):
    data = process_frequency(Frequency[i])
    ref.set({
        'data': {
            'graph' + str(i): data
        },
    }, merge=True)
########################################################################################################################
########################################################################################################################
ref = db.collection('dashboard').document('malteria').collection('pages').document('J7rFlKnaF1XAdVi9bIYM')
ref.set({
    'category': 'Dashboard',
    'name': 'Behaviour',
    'path': '/Comportamiento',
    'type': 'BEHAVIOUR ANALYTICS',
    'icon': 'pie_chart'
})

for i in range(len(Behavior)):
    data = process_behaviour(Behavior[i])
    ref.set({
        'data': {
            'graph' + str(i): data
        },
    }, merge=True)
