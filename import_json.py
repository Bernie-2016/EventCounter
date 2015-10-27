import  json, urllib, itertools
from . import db
    
def insert(rows):
    insertions = []
    for event in rows:
        if 'attendee_count' in event:
            attendee_count = int(event['attendee_count'])
        elif 'shift_details' in event:
            assert len(event['shift_details']) == int(event['shiftcount'])
            # XXX  This  may   multicount  attendees  of  multiple
            # shifts!  Need actual list of attendees.
            attendee_count = sum(int(s['attendee_count'])
                                 for s in event['shift_details'])
        else:
            # No attendee_count information:
            attendee_count = None
        insertion = event.copy()
        insertion['attendee_count'] = 0 if attendee_count is None else attendee_count
        insertion['attendee_info'] = True if attendee_count is not None else False
        insertion['venue_zip'] = insertion['venue_zip'].split('-')[0]
        insertions.append(insertion)
    db.insert_event_counts(insertions)

def import_events(url):
    events = json.loads(urllib.urlopen(url).read())['results']
    insert(events)

def import_daily_events():
    import_events('http://d2bq2yf31lju3q.cloudfront.net/d/events.json')
    
def import_total_events():
    import_events('https://go.berniesanders.com/page/event/search_results?format=json&wrap=no&orderby[0]=date&orderby[1]=desc&event_type=26&mime=text/json&limit=10000&country=*&date_start=1')
    
