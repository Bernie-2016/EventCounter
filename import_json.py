import  json, urllib, itertools, logging
from . import db, bsd
from .data import cl
    
def insert(rows):
    insertions = []
    for event in rows:
        if '-' in event['venue_zip']:
            event['venue_zip'] = event['venue_zip'].split('-')[0]
        if not cl.conus_p(event['venue_zip']):
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
        insertion['attendee_count'] = 0 if attendee_count is None else attendee_count
        insertion['attendee_info'] = True if attendee_count is not None else False
        insertion['venue_zip'] = insertion['venue_zip'].split('-')[0]
        insertion['clregion'] = cl.zipcl[insertion['venue_zip']]
        insertions.append(insertion)
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
    
