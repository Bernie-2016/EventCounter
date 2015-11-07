import os, json, datetime, httplib, time, cPickle, re
from dateutil.parser import parse as parse_date
from bsdapi.BsdApi import Factory
from . import config

api = Factory().create(id=config.bsdid, secret=config.bsdsecret,
                       host=config.bsdhost, port=80, securePort=443)

api.verbose=True

date_format = '%Y-%m-%d %H:%M:%S'

def get_search_results(date):
    # The higher you set this limit, the greater the risk of a network
    # timeout/failure, but the lower you  set it, the greater the risk
    # of too  many events on one  day causing the pagination  to fail.
    # (But that would be a nice problem to have!)
    req = dict(date_start=str(date), limit='13000')
    for dummy in 10*[None]:
        resp = api.doRequest('/event/search_events', req , api.GET, None)
        # Sometimes  you  get a  temporary  'bad  gateway' from  this.
        # retry a few times.
        if not (resp.http_status == 502 and resp.http_reason == 'Bad Gateway'):
            break
            time.sleep(5)
    if resp.http_status != 200:
        raise httplib.HTTPException, 'Event search failed:\n' + str(resp)
    return resp

nonexistent_re = re.compile(
    "The event_id_obfuscated '[a-z0-9]{,10}' does not exist in the system.")

def get_dates(events, date_field='start_dt', default='1972-08-02 06:00:00'):
    return [datetime.datetime.strptime(e.get(date_field, default), date_format)
            for e in events if not nonexistent_re.match(str(e))]

never = datetime.datetime(2500, 1, 1)

def process_events(resp):
    events = json.loads(resp.body)
    events = [e for e in events if e['name'] != 'This is merely a test.']
    dates = get_dates(events)
    assert dates == sorted(dates), 'Events ordered by start_dt'
    by_id = dict((e['event_id_obfuscated'], e) for e in events)
    return events, dates[-1] if dates else never, by_id

def get_events_since(since):
    # Need to do this with start_dt, not create_dt.  Some events don't
    # have  a create_dt!   Also,  pagination is  automatically set  to
    # 2500.  It can handle 10,000 at a time, but there are more events
    # than that.  So need to page through them.
    if 'EVENTCOUNTERCURRENTEVENTS' in os.environ:
        # This  assumes  that  EVENTCOUNTERCURRENTEVENTS points  to  a
        # pickle file with ALL the events up to the given date
        events = cPickle.load(open(os.environ['EVENTCOUNTERCURRENTEVENTS']))
        since = max(parse_date(since), max(get_dates(events, 'create_dt')))
    resp = get_search_results(str(since))
    events, latest, ids = process_events(resp)
    while True:
        resp = get_search_results(latest)
        cevents, newlatest, cids = process_events(resp)
        overlap = set(cids).intersection(set(ids))
        if len(cids) == len(overlap): # Must be no more to get
            break
        events.extend([e for e in cevents
                       if e['event_id_obfuscated'] not in overlap])
        ids.update(cids)
        # If you don't request enough  events, the trick of paginating
        # by the setting date_start to the latest event in the current
        # batch will fail,  because you'll get the same  set of events
        # on two successive requests, and this code will stop.
        assert len(set(e['start_dt'] for e in events)) > 1, 'May need to set the request limit higher'
        latest = newlatest
    return events

def get_available_event_types():
    event_types = json.loads(api.doRequest('/event/get_available_types',
                                           None, api.POST, body={}).body)
    return dict((e['name'], int(e['event_type_id'])) for e in event_types)

