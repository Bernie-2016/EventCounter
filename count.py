import json, BaseHTTPServer, itertools, re, cStringIO, traceback, copy
import threading, os, logging, time, SocketServer, base64, socket, bisect
from dateutil.parser import parse as parse_date
from . import db, mail, import_json

class RequestError(Exception):
    "Raised when there's a problem with a request"

class RequestHandler(BaseHTTPServer.BaseHTTPRequestHandler):

    def aggregate(self, zips, timebreaks, getrows):
        zips = map(frozenset, zips) # zips must be hashable, for counting
        intervals = [dict((k, 0) for k in 'events attendees events_with_attendee_info'.split())
                     for dummy in range(len(timebreaks)-1)]
        counts = dict((z, copy.deepcopy(intervals)) for z in zips)
        while True:
            rows = getrows()
            if not rows:
                break
            for venue_zip, start_dt, attendee_count, attendee_info in rows:
                for zipset, zcounts in counts.items():
                    if venue_zip in zipset:
                        intidx = bisect.bisect(timebreaks, start_dt)
                        # Bisect puts a bin below the lowest value, which we don't need.
                        current_info = zcounts[intidx-1]
                        current_info['events'] += 1
                        if attendee_info:
                            current_info['attendees'] += attendee_count
                            current_info['events_with_attendee_info'] += 1
        return counts

    def get_payload(self):
        length = int(self.headers.getheader('content-length'))
        if length > 1e6:
            raise RequestError, 'Maximum payload 1M'
        try:
            payload = self.rfile.read(length)
        except socket.timeout, e:
            raise RequestError, 'Read of request timed out: ' + e.message
        return payload

    def parse_payload(self, payload):
        try:
            data = json.loads(payload)
        except ValueError, e:
            raise RequestError, e.message
        if not isinstance(data, (list, tuple)) or len(data) != 2:
            raise RequestError, 'Expects a pair: list of zipsets and list of time boundaries'
        zips, timebreaks = data
        if any(not isinstance(z, (tuple, list)) for z in zips):
            raise RequestError, 'zips should be a list of  lists of zips'
        allzips = list(itertools.chain(*zips))
        if len(timebreaks) < 2:
            raise RequestError, 'Have to be at least two time boundaries'
        timebreaks = map(parse_date, timebreaks)
        for e1, e2 in zip(timebreaks, timebreaks[1:]):
            if not e1 < e2:
                raise RequestError, 'Dates must strictly ascend, but %s >= %s' % (e1, e2)
        for z in allzips:
            if not isinstance(z, basestring) and re.match('\d{5}$', z):
                raise RequestError, 'Not a five-digit zip code: %s' % z
        return zips, timebreaks

    def report_error(self, e):
        tb = cStringIO.StringIO()
        traceback.print_exc(file=tb)            
        self.wfile.write(json.dumps({'error': tb.getvalue()}))
        self.send_response(500)
        self.end_headers()
        if not isinstance(e, RequestError):
            # Something went  wrong on our end;  record everything
            # so we can reconstruct it, and let someone know.
            logmsg = '''Error in request from %s:
%s
************************************************************************
request: %r
gzipped database: %r''' % (self.client_address, tb.getvalue(), payload,
                           base64.encode(db.dump()))
            logging.info(logmsg)
            mail.send('Error from bernievents', logmsg)
        
    def do_POST(self):
        # Don't tie up the connection forever if length's too long
        self.rfile._sock.settimeout(10)
        # XXX Connection per request, for multithreading.  Might need a pool?
        conn = db.connection() 
        try:
            payload = self.get_payload()
            zips, timebreaks = self.parse_payload(payload)
            getrows = db.get_counts(zips, timebreaks, conn)
            counts = self.aggregate(zips, timebreaks, getrows)
        except Exception, e:
            if 'payload' not in locals():
                payload = 'Unread payload'
            self.report_error(e)
            raise
        finally:
            conn.close()
        self.send_response(200)
        self.end_headers()
        self.wfile.write(json.dumps({'success': counts.values()}))
        
class ThreadedHTTPServer(SocketServer.ThreadingMixIn, 
                         BaseHTTPServer.HTTPServer):
    """Handle requests in separate threads."""

# Pull an update every hour
def update_db():
    logging.info('Updating database')
    import_json.import_daily_events()
    logging.info('Done updating database')
    threading.Timer(60*60, update_db)

if __name__ == '__main__':
    logdir = '/var/log/bernieevents'
    if not os.path.exists(logdir):
        os.mkdir(logdir)
    logging.basicConfig(filename=os.path.join(logdir, '%s.log' % time.ctime().replace(' ', '-')),
                        level=logging.INFO)
    logging.info('Starting service')
    # Make sure we have a full update to start with
    import_json.import_total_events()
    # Do hourly updates
    update_db()    
    BaseHTTPServer.test(HandlerClass=RequestHandler,
                        ServerClass=ThreadedHTTPServer
                        )

