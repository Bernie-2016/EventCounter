import os, json
from bsdapi.BsdApi import Factory

api = Factory().create(
    id=os.environ['BSDID'], secret=os.environ['BSDSECRET'],
    host=os.environ['BSDHOST'], port=os.environ.get('BSDPORT',None) or '80',
    securePort=os.environ.get('BSDSECUREPORT', None) or '443')

def get_events_since(date):
    resp = api.doRequest('/event/search_events', {'create_date_start': date},
                         api.GET, None)
    return json.loads(resp.body)
