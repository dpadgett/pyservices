#!/usr/bin/python

from os import listdir, stat, utime
from os.path import isfile, isdir, join, exists, dirname, basename
import hashlib
import struct
import re

from dateutil.parser import *
from dateutil.tz import *
from dateutil.relativedelta import *
import datetime

import traceback

from pytz import timezone

def strip_colors(string):
  ''' Returns the string with color codes stripped'''
  idx = 0
  result = ''
  while idx < len(string):
    if string[idx] == '^' and idx < len(string) - 1 and ord(string[idx + 1]) >= ord('0') and ord(string[idx + 1]) <= ord('9'):
      idx += 2
      continue
    result += string[idx]
    idx += 1
  return result

if __name__ == '__main__':
  log = u'/cygdrive/Q/dumbledore3@gmail.com/whoracle_data_2/.local/share/openjk/base/server_internal.log'
  log = u'/tmp/server_internal.log'
  outdir = u'/tmp/logs'
  outfile = None
  with open(log, 'r') as f:
    basedate = parse('Mon Jun 09 19:04:14 2014')
    for linenum, line in enumerate(f):
      if not line.startswith("["):
        print line, 'missing starting ['
        continue
      content = line[line.find(']') + 2:-1]
      if content.startswith("Timestamp: UTC "):
        newbasedate = parse(content[len("Timestamp: UTC "):])
        if newbasedate < basedate:
          print newbasedate, basedate
          break
        basedate = newbasedate
      timestr = line[:line.find(']')]
      timestr = timestr[timestr.rfind('['):]
      timestamp = parse(timestr[1:9])
      timestamp = datetime.datetime.combine(basedate.date(), timestamp.time())
      if (timestamp - basedate) < datetime.timedelta(hours=-12):
        timestamp += datetime.timedelta(days=1)
      if outfile == None or timestamp.date() != basedate.date():
        if outfile != None:
          outfile.close()
        outfile = open(join(outdir, "server.%s.log" % (timestamp.strftime("%Y%m%d-%H%M%S"))), "a")
      basedate = timestamp
      timestamp = timezone('UTC').localize(timestamp, is_dst=False)
      timestamp = timestamp.astimezone(timezone('CET'))
      #print "%s: %s" % (timestamp.strftime('%a %b %d %H:%M:%S %Z %Y'), strip_colors(content))
      outfile.write("%s: %s\n" % (timestamp.strftime('%a %b %d %H:%M:%S %Z %Y'), content))
  if outfile != None:
    outfile.close()