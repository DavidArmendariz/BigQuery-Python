from process_bigquery import Behavior, Frequency, Overview, process_frequency, process_behaviour, process_overview, process_behaviour_comportamiento
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
# ref = db.collection('dashboard').document('malteria').collection('pages').document('mREBXDwkqgk329kHj4Lb')
# ref.set({
#     'category': 'Dashboard',
#     'name': 'Frecuencia',
#     'path': '/Frecuencia',
#     'type': 'FREQUENCY ANALYTICS',
#     'icon': 'show_chart'
# })
#
# for i in range(len(Frequency)):
#     data = process_frequency(Frequency[i])
#     ref.set({
#         'data': {
#             'graph' + str(i): data
#         },
#     }, merge=True)
########################################################################################################################
########################################################################################################################
ref = db.collection('dashboard').document('malteria').collection('pages').document('fufBlU7lzxP84W7kqk0D')
ref.set({
    'category': 'Dashboard',
    'name': 'Behaviour',
    'path': '/Comportamiento',
    'type': 'BEHAVIOUR ANALYTICS',
    'icon': 'pie_chart'
})

for i in range(len(Behavior)):
    data = process_behaviour_comportamiento(Behavior[i])
    ref.set({
        'data': {
            'graph' + str(i): data
        },
    }, merge=True)
########################################################################################################################
########################################################################################################################
# ref = db.collection('dashboard').document('malteria').collection('pages').document('vBSdW81toTVVhCdblZjn')
#
# ref.set({
#     'category': 'Dashboard',
#     'name': 'Overview',
#     'path': '/Overview',
#     'type': 'OVERVIEW ANALYTICS',
#     'icon': 'equalizer'
# })
#
# ref.set({
#     'data': {
#         'graph0': process_overview(Overview[0])
#     },
# }, merge=True)
#
# for i in range(1, len(Overview)):
#     data = process_behaviour(Overview[i])
#     ref.set({
#         'data': {
#             'graph' + str(i): data
#         },
#     }, merge=True)
