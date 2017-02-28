#!/usr/bin/python -u
from subprocess import Popen, PIPE
import time
import sys
import os
import traceback
import urlparse
import zipfile

from pymongo import MongoClient

start = int(sys.argv[1])
end = int(sys.argv[2])

from datetime import datetime
import pytz

start = datetime.fromtimestamp(start, pytz.utc)
end = datetime.fromtimestamp(end, pytz.utc)

db = MongoClient("mongodb").demos

#print 'Looking from', start, 'to', end

basedir = u'/cygdrive/U/demos/'
demos = [demo['_id'].encode('utf8') for demo in db.mindemos.find({'t':{'$gte':start,'$lt':end}},{'_id':1})]

print 'Content-type: application/octet-stream'
print 'Content-Disposition: attachment; filename="demos-' + start.strftime('%Y-%m-%d_%H-%M-%S') + '_-_' + end.strftime('%Y-%m-%d_%H-%M-%S') + '.zip"'
print 'Status: 200 OK'
print ''

zip = Popen(['zip', '-'] + demos, cwd=basedir)
zip.wait()
