import os, urlparse

def get_credentials(path):
    for line in os.popen('heroku config -s'):
        name, val = line.strip().split('=', 1)
        os.environ[name] = val

if os.environ.get('EVENTCOUNTERLOCALCREDENTIALS', None):
    get_credentials(os.environ['EVENTCOUNTERLOCALCREDENTIALS'])
    # Use the local db for now XXX 
    os.environ['CLEARDB_DATABASE_URL'] = 'mysql://root:passme@localhost/bernieevents'

dbinfo = urlparse.urlparse(os.environ['CLEARDB_DATABASE_URL'])
dbcreds, dbhost = dbinfo.netloc.split('@')
dbuser, dbpass = dbcreds.split(':')
database = os.path.split(dbinfo.path)[-1]

bsdid = os.environ['BSDID']
bsdsecret = os.environ['BSDSECRET']
bsdhost = os.environ['BSDHOST']
port=os.environ.get('BSDPORT',None) or '80',
securePort=os.environ.get('BSDSECUREPORT', None) or '443'
