import json, BaseHTTPServer, itertools, bisect, re, cStringIO, traceback, copy
from dateutil.parser import parse as parse_date
from . import db

class RequestHandler(BaseHTTPServer.BaseHTTPRequestHandler):

    def do_POST(self):
        # Don't tie up the connection forever if length's too long
        self.rfile._sock.settimeout(10)
        # XXX Connection per request, for multithreading.  Might need a pool?
        conn = db.connection() 
        try:
            length = int(self.headers.getheader('content-length'))
            if length > 1e6:
                raise ValueError, 'Maximum payload 1M'
            payload = self.rfile.read(length)
            data = json.loads(payload)
            if not isinstance(data, (list, tuple)) or len(data) != 2:
                raise ValueError, 'Expects a pair: list of zipsets and list of time boundaries'
            zips, timebreaks = data
            if any(not isinstance(z, (tuple, list)) for z in zips):
                raise ValueError, 'zips should be a list of  lists of zips'
            allzips = list(itertools.chain(*zips))
            zips = map(frozenset, zips)
            intervals = [dict((k, 0) for k in 'events attendees events_with_attendee_info'.split())
                         for dummy in range(len(timebreaks)-1)]
            counts = dict((z, copy.deepcopy(intervals)) for z in zips)
            if len(timebreaks) < 2:
                raise ValueError, 'Have to be at least two time boundaries'
            timebreaks = map(parse_date, timebreaks)
            for e1, e2 in zip(timebreaks, timebreaks[1:]):
                assert e1 < e2, 'Dates must strictly ascend, but %s >= %s' % (e1, e2)
            cursor = conn.cursor()
            for z in allzips: # Sanitize so we can splice zips straight in to the sql query
                if not isinstance(z, basestring) and re.match('\d{5}$', z):
                    raise ValueError, 'Not a zip code: %s' % z
            cols = 'venue_zip start_dt attendee_count attendee_info'.split()
            cursor.execute('''SELECT %s FROM events WHERE venue_zip IN (%s) AND attendee_info IS TRUE
            AND start_dt >= %%s AND start_dt < %%s;''' % (','.join(cols), ','.join(allzips)),
                           [timebreaks[0], timebreaks[-1]])
            while True:
                rows = cursor.fetchmany(1000)
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
                            
        except Exception, e:
            tb = cStringIO.StringIO()
            traceback.print_exc(file=tb)            
            self.wfile.write(json.dumps({'error': tb.getvalue()}))
            self.send_response(500)
            self.end_headers()
            raise
        finally:
            conn.close()
        self.send_response(200)
        self.end_headers()
        self.wfile.write(json.dumps({'success': [counts[z] for z in zips]}))
        
if __name__ == '__main__':
    BaseHTTPServer.test(HandlerClass=RequestHandler,
                        # ServerClass=ThreadedHTTPServer
                        )

