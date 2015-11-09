import json, itertools, re, cStringIO, traceback, copy, sys, cherrypy
import threading, os, time, SocketServer, base64, socket, bisect, time
import frozendict, cStringIO, gzip
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
                for year in range(2015, 2017)
                for weeknum in range(1, 53)]
assert sorted(weekly_dates) == weekly_dates

def deddict(d):
    return dict((k, deddict(v) if isinstance(v, dict) else v)
                for k, v in d.items())

class Root(object):

    default_aggregate_values = dict(
        # State abbreviations to filter for
        states=frozenset(cl.conus_states),
        # Craigslist regions to filter for
        clregions=frozenset(cl.clzip.keys()),
        # Zips to filter for
        zips=frozenset(cl.zipcl.keys()),
        # Event type ids to filter for
        event_types=frozenset(range(2000)),
        # [date(1),...,date(n)] gives counts for [(date(1),date(2)),...(date(n-1),date(n))]
        timebreaks=tuple(weekly_dates),
        # What to count
        counts=('count', 'rsvp'),
        # What date to use when aggregating
        time_type='create_dt',
        # Ajax string...
        _=None)

    # Weird events which should be excluded
    anomalous = set(['4vfr2'])

    @cachemaker.expiring_lrucache(maxsize=100, timeout=3600, name='aggregate')
    def _aggregate(self, kw):
        event_types_lookup = dict((id, name) for name, id in db.get_event_types().items())
        # Build the return value in this object
             #State        # CL region   # Daterange   #eventtype
        rv = ddict(lambda: ddict(lambda: ddict(lambda: ddict(
            lambda: dict((e,0) for e in kw['counts']))))) # And the actual counts...
        for event in db.get_all_events():
            if event['event_id_obfuscated'] in self.anomalous:
                continue
            interval = bisect.bisect(kw['timebreaks'], event[kw['time_type']])
            if interval == 0 or interval == len(kw['timebreaks']):
                # Event lies outside requested time intervals
                continue
            # If we decide to make smaller queries from the front end,
            # a  lot of  this filtering  could be  moved into  the sql
            # query in db.py to make it faster.  But it's clearer here.
            if not ((event['venue_state_cd'] in kw['states'])    and
                    (event['clregion']       in kw['clregions']) and
                    (event['venue_zip']      in kw['zips'])      and
                    (event['event_type_id']  in kw['event_types'])):
                continue
            datestring = datetime.strftime(kw['timebreaks'][interval-1], '%Y-%m-%d')
            event_type = event_types_lookup[int(event['event_type_id'])]
            ccounts = rv[event['venue_state_cd']][event['clregion']][datestring][event_type]
            for counttype in kw['counts']:
                summand = {'count': 1, 'rsvp': int(event['attendee_count'])}[counttype]
                ccounts[counttype] += summand
        return deddict(rv)

    @cherrypy.expose # Wrapper to deal with incompatible decorations
    def aggregate(self, **kw):
        for k, v in Root.default_aggregate_values.items():
            kw[k] = json.loads(kw[k]) if k in kw else v
        if all(isinstance(tb,  basestring) for tb in kw['timebreaks']):
            kw['timebreaks']  = tuple(map(parse_date, kw['timebreaks']))
        return_str = 'window.aggregatedData=' + json.dumps(
            self._aggregate(frozendict.frozendict(kw)))
        # Return the gzipped file XXX, this is clearly wrong.  What are the minimal headers?
        cherrypy.response.headers["Content-Encoding"] = "gzip"
        cherrypy.response.headers["Vary"] = "Accept-Encoding"
        cherrypy.response.headers["Content-Disposition"] ="gzip"
        cherrypy.response.headers["Content-Type"] ="application/javascript"
        return_file = cStringIO.StringIO()
        with gzip.GzipFile(fileobj=return_file, mode="w") as f:
            f.write(return_str)
        return return_file.getvalue()
        
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
    if not os.environ.get('DONTUPDATEEVENTSDB', None):
        import_json.import_bsd_events_since(since)
        update_db()
    cherrypy.config.update({'server.socket_port': int(sys.argv[1]),
                            'server.socket_host': '0.0.0.0'})
    cherrypy.quickstart(Root())
