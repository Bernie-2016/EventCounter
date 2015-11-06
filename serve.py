import json, itertools, re, cStringIO, traceback, copy, sys, cherrypy
import threading, os, time, SocketServer, base64, socket, bisect, time
from repoze.lru import CacheMaker
from dateutil.parser import parse as parse_date
from datetime import datetime, timedelta
from collections import defaultdict as ddict

# Work around  the fact that that cherry restarts don't load as a package
path = os.path.dirname(__file__)
if path not in sys.path:
    sys.path.append(path)
from EventCounter import db, import_json, bsd
from EventCounter.data import cl

cachemaker = CacheMaker()

weekly_dates = [datetime.strptime('%s Mon %s' % (year, weeknum), '%Y %a %W')
                for weeknum in range(1, 53)
                for year in range(2015, 2017)]

def deddict(d):
    return dict((k, deddict(v) if isinstance(v, dict) else v)
                for k, v in d.items())

class Root(object):
    @cherrypy.expose
    @cachemaker.expiring_lrucache(maxsize=100, timeout=3600, name='aggregate')
    def aggregate(self,
                  # State abbreviations to filter for
                  states=set(cl.conus_states),
                  # Craigslist regions to filter for
                  clregions=set(cl.clzip.keys()),
                  # Zips to filter for
                  zips=set(cl.zipcl.keys()),
                  # Event type ids to filter for
                  event_types=set(range(2000)),
                  # [date(1),...,date(n)] gives counts for [(date(1),date(2)),...(date(n-1),date(n))]
                  timebreaks=weekly_dates,
                  # What to count
                  counts=['events', 'rsvps'],
                  # What date to use when aggregating
                  time_type='create_dt'):
        # Convert any web arguments to json objects.  They arrive as strings.
        states      = set(json.loads(states))      if isinstance(states,      basestring) else states
        clregions   = set(json.loads(clregions))   if isinstance(clregions,   basestring) else clregions
        zips        = set(json.loads(zips))        if isinstance(zips,        basestring) else zips
        event_types = set(json.loads(event_types)) if isinstance(event_types, basestring) else event_types
        counts      = json.loads(counts)           if isinstance(counts,      basestring) else counts
        if isinstance(timebreaks,  basestring):
            timebreaks  = map(parse_date, json.loads(timebreaks))
        event_types_lookup = dict((id, name) for name, id in db.get_event_types().items())
        # Build the return value in this object
             #State        # CL region   # Daterange   #eventtype
        rv = ddict(lambda: ddict(lambda: ddict(lambda: ddict(
            lambda: dict((e,0) for e in counts)))))
        for event in db.get_all_events():
            interval = bisect.bisect(timebreaks, event[time_type])
            if interval == 0 or interval == len(timebreaks):
                # Event lies outside requested time intervals
                continue
            # If we decide to make smaller queries from the front end,
            # a  lot of  this filtering  could be  moved into  the sql
            # query in db.py to make it faster.  But it's clearer here.
            if not ((event['venue_state_cd'] in states)    and
                    (event['clregion']       in clregions) and
                    (event['venue_zip']      in zips)      and
                    (event['event_type_id']  in event_types)):
                continue
            datestring = datetime.strftime(timebreaks[interval-1], '%Y-%m-%d')
            event_type = event_types_lookup[int(event['event_type_id'])]
            ccounts = rv[event['venue_state_cd']][event['clregion']][event_type][datestring]
            for counttype in counts:
                summand = {'events': 1, 'rsvps': int(event['attendee_count'])}[counttype]
                ccounts[counttype] += summand
        return json.dumps(deddict(rv))

# Pull an update every hour
def update_db():
    delay = 60*60 # One hour
    cherrypy.log('Routine database update')
    # Pull updates from twice the delay back, in case of clock skew.
    hourago = datetime.now() - timedelta(seconds = 2*delay)
    update_start = max(hourago, db.most_recently_created_event_date())
    import_json.import_bsd_events_since(update_start.ctime())
    cherrypy.log('Done updating database')
    cachemaker.clear('aggregate')
    threading.Timer(delay, update_db)

if __name__ == '__main__':
    db.maybe_create_tables()
    since = db.most_recently_created_event_date().ctime()
    cherrypy.log('Updating database with events since %s' % since)
    import_json.import_bsd_events_since(since)
    update_db()
    cherrypy.config.update({'server.socket_port': int(sys.argv[1])})
    cherrypy.quickstart(Root())
