# sample.py
import falcon
import json

import StringIO
import time
import sys
import traceback
from subprocess import Popen, PIPE, STDOUT
 
class QuoteResource:
  def on_get(self, req, resp):
    """Handles GET requests"""
    quote = {
      'quote': 'I\'ve always been more interested in the future than in the past.',
      'author': 'Grace Hopper'
    }

    resp.body = json.dumps(quote)

def run(script, args, stdin):
  cwd = '/home/pyservices/'
  if script[0:1] != '/':
    script = cwd + script
  start = time.time()

  # this would be nice, since it doesn't need to subprocess, but we can't stream the output then

  #origargv = sys.argv
  #sys.argv = [script]
  #sys.argv.extend(args)
  #print sys.argv
  #d = dict(locals(), **globals())
  #try:
  #  execfile(script, d, d)
  #except:
  #  print traceback.format_exc()
  #sys.argv = origargv
  #print 'Elapsed:', time.time() - start

  cmd = ['/usr/bin/python', script]
  cmd.extend(args)
  print cmd
  ps = Popen(cmd, stdout=PIPE, stderr=STDOUT, stdin=PIPE)
  # assuming no output would be written until stdin was fully read...
  ps.stdin.write(stdin.read())
  ps.stdin.close()
  # workaround since it seems to hate serving from pipes directly
  def lines():
    for line in ps.stdout:
      #print line
      yield line
  return lines() #ps.stdout

class ExecuteResource:
  def on_get(self, req, resp):
    script = req.get_param("file")
    #logbuf = StringIO.StringIO()
    #origout = sys.stdout
    #sys.stdout = logbuf

    resp.content_type = 'text/plain'
    resp.stream = run(script, req.get_param_as_list("arg") or [], req.stream)

    #sys.stdout = origout
    #resp.body = logbuf.getvalue()
    #logbuf.close()

  def on_post(self, req, resp):
    self.on_get(req, resp)
 
api = falcon.API()
api.add_route('/quote', QuoteResource())
api.add_route('/execute', ExecuteResource())
