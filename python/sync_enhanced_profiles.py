#!/usr/bin/python -u
import sys
import bson.json_util
import enhanced_profile_lib

if __name__ == '__main__':
  mode = 'dryrun'
  if len(sys.argv) > 1 and sys.argv[1] in ('dryrun', 'backfill', 'cron'):
    mode = sys.argv[1]
  summary = enhanced_profile_lib.run_sync(mode=mode)
  print bson.json_util.dumps(summary, indent=2)
