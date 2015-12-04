import MySQLdb, itertools, re, os, bisect, copy, urlparse, contextlib, datetime
from . import config, bsd

schema = {'venue_zip'           : 'CHAR(5)',
          'venue_state_cd'      : 'CHAR(2)',
          'clregion'            : 'VARCHAR(20)',
          'creator_cons_id'     : 'VARCHAR(20)',
          'attendee_count'      : 'INT',
          'create_dt'           : 'DATETIME',
          'start_dt'            : 'DATETIME',
          'attendee_info'       : 'BOOLEAN',
          'event_type_id'       : 'INT',
          'event_id_obfuscated' : 'VARCHAR(20)',}
primary_key = 'event_id_obfuscated'
assert primary_key in schema

db_fields = schema.keys()

def connection():
    return MySQLdb.connect(passwd=config.dbpass, db=config.database,
                           user=config.dbuser, host=config.dbhost)

def insert_event_counts(insertions, db=None):
    for row in insertions:
        assert re.match('\d{5}$', row['venue_zip'])
    _db = db or connection()
    cursor = _db.cursor()
    try:
        # Do an  upsert into  the DB.   PRIMARY KEY  is event_id_obfuscated.
        # DUPLICATE KEY UPDATE clause  simply assigns all the received
        # values to their respective columns.
        cursor.executemany(
            '''INSERT INTO events (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s;''' % (
            # E.g., 'venue_zip, create_dt, attendee_count, attendee_info, event_id_obfuscated'
            ', '.join(db_fields),
            # E.g. '%s, %s, %s, %s, %s'
            ', '.join(['%s']*len(db_fields)),
            # E.g. 'venue_zip = VALUES(venue_zip), create_dt = VALUES(create_dt), ...'
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

def get_all_events():
    with contextlib.closing(connection()) as conn: # Close connection automatically
        with conn as cursor:
            cursor.execute('SELECT %s FROM events' % ','.join(schema))
            while True:
                results = [dict(zip(schema, r)) for r in cursor.fetchmany(1000)]
                if results:
                    for row in results:
                        yield row
                else:
                    break

def most_recently_created_event_date():
    with contextlib.closing(connection()) as conn: # Close connection automatically
        with conn as cursor:
            cursor.execute('SELECT MAX(create_dt) FROM events;')
            rv = cursor.fetchone()[0]
            return datetime.datetime(1972, 8, 2) if rv is None else rv
                

def maybe_create_tables():
    with contextlib.closing(connection()) as conn:
        with conn as cursor:
            cursor.execute(
                '''CREATE TABLE IF NOT EXISTS events (%s, PRIMARY KEY (%s));''' % (
                ', '.join('%s %s' % (n, t) for n,t in schema.items()), primary_key))
            cursor.execute(
                'CREATE TABLE IF NOT EXISTS event_types '
                '(id INT, description VARCHAR(255), PRIMARY KEY (id))')
            cursor.execute(
                'CREATE TABLE IF NOT EXISTS event_creators '
                '(id INT, firstname VARCHAR(255), lastname VARCHAR(255), PRIMARY KEY(id))')

def get_event_types():
    event_types = bsd.get_available_event_types()
    with contextlib.closing(connection()) as conn:
        with conn as cursor:
            # Check that there are no mismatches in descriptions
            cursor.execute('SELECT description, id FROM event_types')
            event_types.update(dict(cursor.fetchall()))
    return event_types

def update_event_types(events):
    with contextlib.closing(connection()) as conn:
        with conn as cursor:
            # Check that there are no mismatches in descriptions
            results = get_event_types()
            for description, _id in events.items():
                assert results.get(_id, description) == description
            # Record new event types
            cursor.executemany('INSERT INTO event_types (id, description) VALUES (%s,%s)',
                               [(_id, description) for description, _id in events.items()
                                if _id not in results.values()])

def dump():
    return os.popen('mysqldump -u%s -p%s --host=%s %s | gzip -9' % (
        config.dbuser, config.dbpass, config.dbhost, config.database)).read()

def get_missing_creators(creator_ids):
    """Return the elements of creator_ids  which do not already appear
    in the event_creators table."""
    with contextlib.closing(connection()) as conn:
        with conn as cursor:
            if creator_ids:
                cursor.execute('SELECT id FROM event_creators WHERE id IN %s', (tuple(creator_ids),))
                known_ids = set(cursor.fetchall())
            else:
                known_ids = set()
            return set(creator_ids) - known_ids

def get_all_creators():
    with contextlib.closing(connection()) as conn:
        with conn as cursor:
            cursor.execute('SELECT creator_cons_id FROM events')
            return set(int(r[0]) for r in cursor.fetchall())

def update_creators(creator_ids):
    creators = bsd.get_creators_iter(get_missing_creators(creator_ids))
    with contextlib.closing(connection()) as conn:
        with conn as cursor:
            cursor.executemany('INSERT INTO event_creators (id, firstname, lastname) VALUES (%s,%s,%s)',
                               [tuple(c[k] for k in 'id firstname lastname'.split()) for c in creators])
            
