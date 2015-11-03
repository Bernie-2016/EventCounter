from __future__ import print_function
import heroku, sys, os

print(sys.argv)
username, password = sys.argv[1:]
client = heroku.from_pass(username, password)
api_key = client._api_key

netrc = '''machine %s.heroku.com
  login %s
  password %s
'''
netrc_path = os.path.expanduser('~vagrant/.netrc')
netrc_file = os.fdopen(os.open(netrc_path, os.O_WRONLY| os.O_CREAT, 0600), 'w')
try:
    for machine in ['api', 'git']:
        print(netrc % (machine, username, api_key), file=netrc_file)
except Exception, e:
    netrc_file.close()
    os.remove(netrc_path)
    
