import MySQLdb, itertools, re

bsd_fields  ='''start_day   venue_zip  capacity   name  attendee_count
    shifttimes    start_time   location    longitude   event_type_name
    shiftcount  url  latitude  start_dt is_official  id  shift_details
    id_obfuscated'''.split()

db_fields = 'venue_zip start_dt attendee_count attendee_info id_obfuscated'.split()

def get_row_id(row):
    return row['id_obfuscated']

def connection():
    return MySQLdb.connect(passwd='passme', db='bernieevents', user='root')

def seen_ids(rows, db=None):
    these_ids = [get_row_id(e) for e in rows]
    for this_id in these_ids:
        # Sanitize as pure alphanumeric for direct splicing into DB query
        if not re.match('[A-z0-9]+$', this_id):
            raise ValueError, 'Not a valid id: %s' % this_id
    db = db or connection()
    try:
        cursor = db.cursor()
        cursor.execute('SELECT id_obfuscated FROM events WHERE id_obfuscated IN (%s);' %
                       ','.join("'%s'" % i for i in these_ids))
        return set(itertools.chain(*cursor.fetchall()))
    finally:
        db.close()

def insert_event_counts(insertions, db=None):
    for row in insertions:
        assert re.match('\d{5}$', row['venue_zip'])
    db = db or connection()
    cursor = db.cursor()
    try:
        cursor.executemany(
            '''INSERT INTO events (%s) VALUES (%s)''' % (
            ', '.join(db_fields), ', '.join(['%s']*len(db_fields))),
            [[e[f] for f in db_fields] for e in insertions])
        db.commit()
    except:
        db.rollback()
        raise
    finally:
        db.close()
