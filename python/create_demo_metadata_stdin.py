#!/usr/bin/python

from subprocess import Popen, PIPE
from os import listdir, stat, utime
from os.path import isfile, isdir, join, exists
import locale
import sys
import time
import find_demos

#exit(0)

#demoparser = u'/cygdrive/C/Users/dan/Documents/Visual Studio 2010/Projects/JKDemoMetadata/Release/JKDemoMetadata.exe'
demoparser = u'/home/pyservices/jkdemometadata'

print 'Processing demo'
proc = Popen([demoparser, '-'])
proc.wait()
