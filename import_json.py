import  json, urllib, itertools, logging, cherrypy
from datetime import datetime
from dateutil.parser import parse as parse_date
from . import db, bsd
from .data import cl
    
def get_attendee_count(event):
    # Sometimes this can be None.
    return event.get('attendee_count', None) or 0

def insert(rows):
    event_types = {}
    insertions = []
    for event in rows:
        if bsd.nonexistent_re.match(str(event)):
            continue # This is an error message, not an event
        _zip = event['venue_zip']
        if '-' in _zip:
            _zip = event['venue_zip'] = event['venue_zip'].split('-')[0]
        if _zip not in cl.ziploc:
            # Some zips are  still not included.  To  catch these, use
            # the  softwaretools  db,  instead.   But  this  might  be
            # encumbered by copyright.
            logging.info('Excluding event in unrecognized zip: %s' % event)
            continue
        if not cl.conus_p(_zip):
            logging.info('Excluding non-CONUS event: %s' % event)
            continue
        attendee_count = None # Sentinel for no attendee-count info
        if 'days' in event:
            attendee_count = 0
            for d in event['days']:
                if 'shifts' in d:
                    for s in d['shifts']:
                        attendee_count += int(s['guest_count'])
                else:
                    attendee_count += int(d['guest_count'])
        elif event.get('attendee_count', None) is not None:
            attendee_count = int(event['attendee_count'])
        insertion = event.copy()
        # Normalize state field to venue_state_cd
        insertion['venue_state_cd'] = insertion.get(
            'venue_state_cd', insertion.get(
            'venue_state_code', cl.ziploc.get(insertion['venue_zip'], [None])[0]))
        if insertion['venue_state_cd'] != cl.ziploc[insertion['venue_zip']][0]:
            # At time of writing, this skips about 28 events.
            cherrypy.log('Skipping event with mismatched zip and state: %s' % event['event_id_obfuscated'])
            continue
        # Normalize start_dt to top-level field
        insertion['start_dt'] = insertion.get(
            'start_dt', insertion.get('days', [{'start_dt': None}])[0]['start_dt'])
        assert insertion['start_dt'] is not None
        # Use earliest of now() and start_dt for create_dt if no value given.
        insertion['create_dt'] = str(min(
            datetime.now(), parse_date(insertion.get(
            'create_dt', insertion['start_dt']))))
        insertion['attendee_count'] = 0 if attendee_count is None else attendee_count
        insertion['attendee_info'] = True if attendee_count is not None else False
        insertion['clregion'] = cl.zipcl[insertion['venue_zip']]
        insertions.append(insertion)
        if 'event_type_name' in insertion:
            # Some events have event_type_name, some don't.  Track the
            # ids of  those which do  so we can  fill it in  for those
            # which don't.
            assert 'event_type_id' in insertion # This isn't true for the public interface, but ignore that
            name, _id = insertion['event_type_name'], int(insertion['event_type_id'])
            assert event_types.get(name, _id) == _id
            event_types[name] = _id
    # XXX these database interactions should be wrappen in a commit.
    db.update_event_types(event_types)
    db.insert_event_counts(insertions)

def import_events(url):
    events = json.loads(urllib.urlopen(url).read())['results']
    insert(events)

def import_daily_events():
    import_events('http://d2bq2yf31lju3q.cloudfront.net/d/events.json')
    
def import_total_events():
    import_events('https://go.berniesanders.com/page/event/search_results?format=json&wrap=no&orderby[0]=date&orderby[1]=desc&event_type=26&mime=text/json&limit=10000&country=*&date_start=1')
    
def import_bsd_events_since(date):
    insert(bsd.get_events_since(date))
    
