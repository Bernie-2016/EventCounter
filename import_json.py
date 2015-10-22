import itertools
from . import db
    
def insert(rows):
    observed_fields = itertools.chain(*set(tuple(e.keys()) for  e in  events))
    assert set(observed_fields) == set(db.bsd_fields), 'Expected schema?'
    # Get the rows in the json list which aren't already present in the db
    seen = db.seen_ids(rows)
    unseen = [e for e in rows if db.get_row_id(e) not in set(seen)]
    insertions = []
    for event in unseen:
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
        insertions.append(insertion)
    db.insert_event_counts(insertions)

if __name__ == '__main__':
    import  json, urllib
    events = json.loads(urllib.urlopen('http://d2bq2yf31lju3q.cloudfront.net/d/events.json').read())['results']
    insert(events)
