import os, re
from collections import defaultdict as ddict

# Taken from http://pe.usps.gov/Archive/HTML/DMMArchive0106/L002.htm
path = os.path.join(os.path.dirname(__file__), 'zip_prefixes.txt')

zip_state = {}

pairs = [line.split() for line in open(path)]
zip_state[prefix] = dict((prefix, state) for state, prefix in pairs)

state_zip = ddict(list)

for prefix, state in zip_state.items():
    state_zip[state].append(prefix)

state_regexps = {}

for state, prefixes in state_zip.items():
    state_regexps[state] = re.compile('^(%s)' % '|'.join(prefixes))

    
