#!/bin/bash
set -ex

for v in DBPASSWORD EMAILUSERNAME EMAILPASSWORD EMAILRECIPIENT ; do
    if [ -z "${!v}" ] ; then         
        echo 'Pass credentials as environment variables: ' $v;
        /bin/false;
    fi;
done

# Set mysql password
debconf-set-selections <<< "mysql-server mysql-server/root_password password $DBPASSWORD"
debconf-set-selections <<< "mysql-server mysql-server/root_password_again password $DBPASSWORD"

apt-get update
apt-get -y install mysql-server mysql-client python-mysqldb git python-dateutil < /dev/null

# Create the database & table
mysql -uroot -ppassme << eof
CREATE database IF NOT EXISTS bernieevents; 
use bernieevents;
CREATE TABLE IF NOT EXISTS events (venue_zip CHAR(5), start_dt DATETIME, 
  attendee_count INT, attendee_info BOOLEAN, id_obfuscated VARCHAR(20), 
  PRIMARY KEY (id_obfuscated));
eof

# Get the latest version of the code
git clone https://github.com/Bernie-2016/EventCounter
export module_location="`pwd`"

# Set credentials for email-notification gmail account
cat > EventCounter/email.secret <<EOF
$EMAILUSERNAME
$EMAILPASSWORD
$EMAILRECIPIENT
EOF

# Update/populate the db
python -c 'from EventCounter.import_json import import_total_events; import_total_events();'

# Set up upstart
upstart_file=/etc/init/bernieevents.conf
cat > $upstart_file <<EOF
start on runlevel [2345]
stop on runlevel [016]

respawn

env PYTHONPATH="$module_location"
exec python -m EventCounter.count
EOF

init-checkconf $upstart_file
initctl start bernieevents

