import MySQLdb, itertools, re, os, bisect, copy, urlparse

dbinfo = urlparse.urlparse(os.environ['CLEARDB_DATABASE_URL'])
dbcreds, dbhost = dbinfo.netloc.split('@')
dbuser, dbpass = dbcreds.split(':')
database = os.path.split(dbinfo.path)[-1]

db_fields = 'venue_zip start_dt attendee_count attendee_info id_obfuscated'.split()

def connection():
    return MySQLdb.connect(passwd=dbpass, db=database, user=dbuser, host=dbhost)

def insert_event_counts(insertions, db=None):
    for row in insertions:
        assert re.match('\d{5}$', row['venue_zip'])
    _db = db or connection()
    cursor = _db.cursor()
    try:
        # Do an  upsert into  the DB.   PRIMARY KEY  is id_obfuscated.
        # DUPLICATE KEY UPDATE clause  simply assigns all the received
        # values to their respective columns.
        cursor.executemany(
            '''INSERT INTO events (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s;''' % (
            # E.g., 'venue_zip, start_dt, attendee_count, attendee_info, id_obfuscated'
            ', '.join(db_fields),
            # E.g. '%s, %s, %s, %s, %s'
            ', '.join(['%s']*len(db_fields)),
            # E.g. 'venue_zip = VALUES(venue_zip), start_dt = VALUES(start_dt), ...'
            ', '.join('%(f)s = VALUES(%(f)s)' % {'f': f} for  f in db_fields)),
            # These values replace the '%s' substrings from the second argument.
            [[e[f] for f in db_fields] for e in insertions])
        _db.commit()
    except:
        _db.rollback()
        raise
    finally:
        if not db: # This is a local connection, don't tie it up
            _db.close()

def get_counts(zips, timebreaks, conn):

    '''Return a zero-argument function which returns rows from the the
    db query until  all such rows are consumed.  The  db query returns
    the counts for  all events between the two times  in timebreaks in
    any of the zips listed in `zips`.'''
    
    cols = 'venue_zip start_dt attendee_count attendee_info'.split()
    allzips = list(itertools.chain(*zips))
    cursor = conn.cursor()
    # Sanitize so we can splice zips straight in to the sql query
    for z in allzips: 
        assert isinstance(z, basestring) and re.match('\d{5}$', z), \
               'Five-digit zip code: %s?' % z
    cursor.execute('''SELECT %s FROM events WHERE venue_zip IN (%s) AND attendee_info IS TRUE
    AND start_dt >= %%s AND start_dt < %%s;''' % (
        # These are the column names, e.g. 'venue_zip, start_dt, attendee_count, attendee_info'
        ','.join(cols),
        # E.g., '45349,02139,...'
        ','.join(allzips)),
                   # The  '%%s' instances  in the  above interpolation
                   # become  '%s'  when  it's  completed,  part  of  a
                   # prepared  statement   in  which  the   dates  are
                   # inserted.
                   [timebreaks[0], timebreaks[-1]])
    return lambda: cursor.fetchmany(1000)
    
def dump():
    return os.popen('mysqldump -u%s -p%s --host=%s %s | gzip -9' % (
        dbuser, dbpass, dbhost, database)).read()
