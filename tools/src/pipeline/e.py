#!/usr/bin/python
import os

body = 'body text'
subject = 'email subject'
toAddress = 'eblipsky@geisinger.edu'

email_cmd = 'echo "'+body+'" | mail -s "'+subject+'" '+toAddress

os.system(email_cmd)

