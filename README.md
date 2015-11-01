Installation
============

This is set up to run under heroku.  It expects the following  config
settings: 

- `EMAILUSERNAME`: Name of gmail account for sending failure
  notifications.  Go
  [here](https://www.google.com/settings/security/lesssecureapps) as
  that google account and choose "turn on."  Otherwise, notifications
  won't work.
- `EMAILPASSWORD`: Password for gmail account
- `EMAILRECIPIENT`: Email to send notifications to
- `BSDHOST`: Host of the Blue State Digital database
- `BSDPORT`: Port for DB
- `BSDID`: ID for DB
- `BSDSECRET`: Secret for DB

Usage
=====

```
% curl -H "Content-Type: application/json" -X POST -d \
    '[[["37209", "37416"]], ["Jan 1 1979", "Oct 23 2015"]]' \
    http://${SERVER_HOSTNAME}:8000/
{"success": [[{"events_with_attendee_info": 1, "attendees": 45, "events": 1}]]}
```

This curl command sends a POST request to the service with payload
`[[["37209", "37416"]], ["Jan 1 1979", "Oct 23 2015"]]`.

The service expects a json list with two elements.  

The first element is a list of lists of zipcodes.  In this case, there
is just one list, `["37209", "37416"]`.  Counts are aggregated over
these zipcode groups.  

The second element of the top list is a list of strictly increasing
dates, in this case `["Jan 1 1979", "Oct 23 2015"]`.  The event counts
over each region within each time interval are returned.  If there are
`n` dates, there will be `n-1` time intervals.

If the request succeeds, a json map with a "success" key is returned.
The value will be a list containing a list per zipcode group, each of
which contains a list of maps containing info about the number of events
in the given zipcode group, one such map for each time interval.

In this case, there is just one zipcode group, and one time interval, so
there's just one such map:

```
{"events_with_attendee_info": 1, "attendees": 45, "events": 1}
```

"attendees" contains the aggregated attendees over the given zips in the
given duration.  "events" is the total number of events, and
"events_with_attendee_info" is the number of events which contributed to
the "attendees" value because they contained attendee info.

More complex example with less prose
====================================

Here's a slightly more complex example, in case that prose was as
impenetrable as it sounds to me:

```
      Zip group A      Zip Group B                 Duration 1    Duration 2
[[["37209", "37416"],["45349","02139"]], ["Jan 1 1979", "Oct 23 2015", "Jan 20 2017"]]
->
{'success': [[{'attendees': 45, 'events': 1, 'events_with_attendee_info': 1},  #(A,1)
              {'attendees': 0, 'events': 0, 'events_with_attendee_info': 0}],  #(A,2)
             [{'attendees': 10, 'events': 1, 'events_with_attendee_info': 1},  #(B,1)
              {'attendees': 2, 'events': 2, 'events_with_attendee_info': 2}]]} #(B,2)
```

Code structure
==============

The entry point is `count.py`.  To run the service in development:

```
% python -m EventCounter.count
```

Pulling counts from the BSD API and storing locally is in
`import_json.py`.  Parsing of the json request is in
`count.RequestHandler.parse_payload`.  Pulling out the counts is in
`db.get_counts`.  The main logic is in `count.RequestHandler.aggregate`.
Everything which touches the db is in `db.py`.

When the code first starts, it pulls events from the BSD api as far back
as it can.  Then once an hour it requests all events from the last two
hours.  It uses `event_id_obfuscated` as the primary key, and upserts
the BSD data.
