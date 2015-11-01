# Import smtplib for the actual sending function
import smtplib, os
from email.mime.text import MIMEText

envnames = 'EMAILUSERNAME EMAILPASSWORD EMAILRECIPIENT'.split()
username, password, recipient = [os.environ[v] for v in envnames]

sender = username+'@gmail.com'

# Need to go to https://www.google.com/settings/security/lesssecureapps and "turn on"
# in sending account, for this to work.
def send(subject, message):
    msg = MIMEText(message)
    for k,v in {'Subject': subject, 'From': sender, 'To': recipient}.items():
        msg[k] = v
    s = smtplib.SMTP('smtp.gmail.com:587')
    s.ehlo()
    s.starttls()
    s.login(sender, password)
    s.sendmail(sender, [recipient], msg.as_string())
    s.quit()
