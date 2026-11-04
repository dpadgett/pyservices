#!/usr/bin/python -u
import sys
import os
import time
import json
import hashlib
import fcntl
import urlparse
import shutil
from subprocess import Popen, PIPE

args = urlparse.parse_qs(urlparse.urlparse(os.environ.get('REQUEST_URI', '')).query)

if 'demo' not in args or len(args['demo']) != 1:
  print 'No demo specified'
  exit()

demo = args['demo'][0]
if (demo[-6:-1] != '.dm_2' and demo[-8:] != '.dm_meta') or demo[0:18] != '/cygdrive/U/demos/' or '/../' in demo:
  print 'Not a demo'
  exit()

if not os.path.isfile(demo):
  print 'Content-type: text/plain'
  print 'Status: 404 Not Found'
  print ''
  print 'File', demo, 'not found'
  exit()

fmt = args.get('format', [''])[0]

if fmt in ('mme', 'mme_init', 'mme_meta'):
  cache_dir = '/home/pyservices/mme_cache'
  try:
    if not os.path.isdir(cache_dir):
      os.makedirs(cache_dir)
  except Exception:
    pass

  st = os.stat(demo)
  cache_key = hashlib.sha1('%s:%d:%d:v1' % (demo, int(st.st_mtime), st.st_size)).hexdigest()
  mme_path = os.path.join(cache_dir, cache_key + '.mme')
  meta_path = os.path.join(cache_dir, cache_key + '.meta.json')
  lock_path = os.path.join(cache_dir, cache_key + '.lock')

  def prune_cache():
    try:
      entries = []
      for fn in os.listdir(cache_dir):
        if fn.endswith('.mme'):
          fp = os.path.join(cache_dir, fn)
          try:
            entries.append((os.path.getmtime(fp), fn[:-4]))
          except Exception:
            pass
      if len(entries) > 60:
        entries.sort()
        for _, base in entries[:len(entries) - 50]:
          if base == cache_key:
            continue
          for ext in ('.mme', '.meta.json', '.lock'):
            try:
              os.unlink(os.path.join(cache_dir, base + ext))
            except Exception:
              pass
    except Exception:
      pass

  def ensure_conversion():
    if os.path.isfile(meta_path) and os.path.isfile(mme_path) and os.path.getsize(mme_path) > 16:
      return
    lock_fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0666)
    try:
      fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except IOError:
      os.close(lock_fd)
      return
    # We acquired the lock; check again if already completed
    if os.path.isfile(meta_path) and os.path.isfile(mme_path) and os.path.getsize(mme_path) > 16:
      os.close(lock_fd)
      return
    prune_cache()
    try:
      if os.path.isfile(mme_path):
        os.unlink(mme_path)
    except Exception:
      pass
    try:
      if os.path.isfile(meta_path):
        os.unlink(meta_path)
    except Exception:
      pass
    converter = '/home/pyservices/dm26tomme'
    devnull = open('/dev/null', 'wb')
    def _child_setup():
      try:
        os.setsid()
      except Exception:
        pass
      for fd in xrange(3, 256):
        if fd != lock_fd:
          try:
            os.close(fd)
          except Exception:
            pass
    # Child inherits only lock_fd so the flock stays held until dm26tomme exits
    Popen([converter, demo, mme_path, meta_path], stdin=devnull, stdout=devnull, stderr=devnull, close_fds=False, preexec_fn=_child_setup)
    devnull.close()
    os.close(lock_fd)

  ensure_conversion()

  if fmt == 'mme_init':
    init_bytes = 524288
    if 'bytes' in args:
      try:
        init_bytes = max(4096, min(4194304, int(args['bytes'][0])))
      except Exception:
        pass
    deadline = time.time() + 60.0
    while time.time() < deadline:
      done = os.path.isfile(meta_path)
      cur_size = os.path.getsize(mme_path) if os.path.isfile(mme_path) else 0
      if cur_size >= init_bytes or (done and cur_size > 16):
        break
      time.sleep(0.015)
    if not os.path.isfile(mme_path):
      print 'Content-type: text/plain'
      print 'Status: 500 Internal Server Error'
      print ''
      print 'Failed to initialize MME stream'
      exit()
    with open(mme_path, 'rb') as f:
      data = f.read(init_bytes)
    print 'Content-type: application/octet-stream'
    print 'Cache-Control: no-store'
    print 'Content-Length: ' + str(len(data))
    print 'Status: 200 OK'
    print ''
    sys.stdout.write(data)
    sys.stdout.flush()
    exit()

  if fmt == 'mme_meta':
    deadline = time.time() + 60.0
    while time.time() < deadline:
      if os.path.isfile(meta_path) and os.path.getsize(meta_path) > 0:
        break
      time.sleep(0.02)
    if not os.path.isfile(meta_path):
      print 'Content-type: text/plain'
      print 'Status: 500 Internal Server Error'
      print ''
      print 'Failed to read MME metadata'
      exit()
    try:
      os.utime(mme_path, None)
      os.utime(meta_path, None)
    except Exception:
      pass
    with open(meta_path, 'rb') as f:
      data = f.read()
    print 'Content-type: application/json'
    print 'Cache-Control: no-store'
    print 'Content-Length: ' + str(len(data))
    print 'Status: 200 OK'
    print ''
    sys.stdout.write(data)
    sys.stdout.flush()
    exit()

  if fmt == 'mme':
    req_start = None
    req_end = None
    is_http_range = False
    http_range = os.environ.get('HTTP_RANGE', '')
    if http_range.startswith('bytes='):
      parts = http_range[6:].split(',')[0].strip().split('-')
      if len(parts) == 2:
        try:
          req_start = int(parts[0]) if parts[0] else 0
          req_end = int(parts[1]) if parts[1] else None
          is_http_range = True
        except Exception:
          req_start = None
          req_end = None
    if req_start is None and 'start' in args:
      try:
        req_start = int(args['start'][0])
        if 'end' in args:
          req_end = int(args['end'][0])
      except Exception:
        req_start = None
        req_end = None

    deadline = time.time() + 60.0
    if req_start is not None:
      need_size = (req_end + 1) if req_end is not None else None
      while time.time() < deadline:
        done = os.path.isfile(meta_path)
        cur_size = os.path.getsize(mme_path) if os.path.isfile(mme_path) else 0
        if done or (need_size is not None and cur_size >= need_size):
          break
        time.sleep(0.015)
      if not os.path.isfile(mme_path):
        print 'Content-type: text/plain'
        print 'Status: 500 Internal Server Error'
        print ''
        print 'Failed to read MME range'
        exit()
      cur_size = os.path.getsize(mme_path)
      done = os.path.isfile(meta_path)
      if req_end is None or (done and req_end >= cur_size):
        req_end = cur_size - 1
      length = max(0, req_end - req_start + 1)
      with open(mme_path, 'rb') as f:
        f.seek(req_start)
        data = f.read(length)
      total_str = str(cur_size) if done else '*'
      print 'Content-type: application/octet-stream'
      print 'Accept-Ranges: bytes'
      print 'Cache-Control: no-store'
      print 'Content-Length: ' + str(len(data))
      if is_http_range:
        actual_end = req_start + len(data) - 1 if len(data) > 0 else req_start
        print 'Content-Range: bytes %d-%d/%s' % (req_start, actual_end, total_str)
        print 'Status: 206 Partial Content'
      else:
        print 'Status: 200 OK'
      print ''
      sys.stdout.write(data)
      sys.stdout.flush()
      exit()
    else:
      while time.time() < deadline:
        if os.path.isfile(meta_path) and os.path.isfile(mme_path):
          break
        time.sleep(0.02)
      if not os.path.isfile(mme_path):
        print 'Content-type: text/plain'
        print 'Status: 500 Internal Server Error'
        print ''
        print 'Failed to read MME file'
        exit()
      cur_size = os.path.getsize(mme_path)
      print 'Content-type: application/octet-stream'
      print 'Accept-Ranges: bytes'
      print 'Content-Length: ' + str(cur_size)
      print 'Status: 200 OK'
      print ''
      with open(mme_path, 'rb') as f:
        shutil.copyfileobj(f, sys.stdout)
      sys.stdout.flush()
      exit()

print 'Content-type: application/octet-stream'
print 'Content-Disposition: attachment; filename="' + os.path.basename(demo) + '"'
print 'Status: 200 OK'
print ''

#with open(demo, 'r') as f:
#  shutil.copyfileobj(f, sys.stdout)

demosanitizer = u'/home/pyservices/demosanitizer'
args = [demosanitizer, '-', '-']

print >> sys.stderr, ' '.join(args)

demofd = open( demo, u'rb' )
proc = Popen(args, stdout=sys.stdout, stdin=demofd)
proc.wait()
demofd.close()
