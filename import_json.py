import  json, urllib, itertools, logging
from datetime import datetime
from dateutil.parser import parse as parse_date
from . import db, bsd
from .data import cl
    
def insert(rows):
    event_types = {}
    insertions = []
    for event in rows:
        if bsd.nonexistent_re.match(str(event)):
            continue # This is an error message, not an event
        _zip = event['venue_zip']
        if '-' in _zip:
            _zip = event['venue_zip'] = event['venue_zip'].split('-')[0]
        if _zip not in cl.zipstate:
            # Some zips are  still not included.  To  catch these, use
            # the  softwaretools  db,  instead.   But  this  might  be
            # encumbered by copyright.
            logging.info('Excluding event in unrecognized zip: %s' % event)
            continue
        if not cl.conus_p(_zip):
            logging.info('Excluding non-CONUS event: %s' % event)
            continue
        if 'shift_details' in event:
            assert 'attendee_count' not in event
            assert len(event['shift_details']) == int(event['shiftcount'])
            # XXX  This  may   multicount  attendees  of  multiple
            # shifts!  Need actual list of attendees.
            attendee_count = sum(int(s['attendee_count'])
                                 for s in event['shift_details'])
        else:
            attendee_count = event.get('attendee_count', None)
            if attendee_count is not None:
                attendee_count = int(attendee_count)
        insertion = event.copy()
        # Normalize state field to venue_state_cd
        insertion['venue_state_cd'] = insertion.get(
            'venue_state_cd', insertion.get('venue_state_code', None))
        assert insertion['venue_state_cd'] is not None
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
            name, _id = insertion['event_type_name'], int(insertion['event_type_id'])
            assert event_types.get(name, _id) == _id
            event_types[name] = _id
    db.insert_event_counts(insertions)
    db.update_event_types(event_types)

def import_events(url):
    events = json.loads(urllib.urlopen(url).read())['results']
    insert(events)

def import_daily_events():
    import_events('http://d2bq2yf31lju3q.cloudfront.net/d/events.json')
    
def import_total_events():
    import_events('https://go.berniesanders.com/page/event/search_results?format=json&wrap=no&orderby[0]=date&orderby[1]=desc&event_type=26&mime=text/json&limit=10000&country=*&date_start=1')
    
def import_bsd_events_since(date):
    insert(bsd.get_events_since(date))
    
