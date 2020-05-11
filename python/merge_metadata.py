import struct

def map_match_hash( map ):
  if 'match_hash' in map:
    return map['match_hash']
  #print 'missing hash:', map
  # compute a hash to identify this match
  match_id = ((map['serverId'] & 0xFFFFFFFF) << 32) + (map['checksumFeed'] & 0xFFFFFFFF)
  match_hash = struct.pack('!Q', match_id).encode('hex')
  return match_hash

def findmap(maps, match_hash):
  longestmapidx = 0
  longestmap = maps[0]
  for idx, map in enumerate(maps):
    if map_match_hash(map) == match_hash:
      return (idx, map)
  raise Exception("Couldn't find map with hash %s" % (match_hash))

def merge_history(datas, matchid, name):
  minversion = min([d['version'] for d in datas])
  keysuffix = ''
  if minversion >= 4:
    keysuffix = '_raw'
  history = {}
  if name == 'newmod':
    names = name
    valuename = 'newmod_id'
  else:
    names = name + 's'
    valuename = name
  sort_values = lambda value_list: value_list.sort(cmp = lambda x, y: x[name + '_start_time' + keysuffix] - y[name + '_start_time' + keysuffix])
  for data in datas:
    mapidx, map = findmap(data['maps'], matchid)
    for (client_id, value_list) in map.get(names, {}).items():
      if ( not history.has_key(client_id) ):
        history[client_id] = []
      for value in value_list:
        if value[name + '_start_time' + keysuffix] > value[name + '_end_time' + keysuffix]:
          continue
        for current_value in history[client_id]:
          if ( not valuename in value ):
            if valuename == 'newmod_id':
              value[valuename] = ''
            else:
              raise Exception('Error: value missing name: ' + str(value) + ' for client ' + str(data['client']['id']) + ' on map ' + map['mapname'])
          if ( value[name + '_start_time' + keysuffix] <= current_value[name + '_end_time' + keysuffix] and
              value[name + '_end_time' + keysuffix] >= current_value[name + '_start_time' + keysuffix] and
              value[valuename] == current_value[valuename] ):
            current_value[name + '_end_time' + keysuffix] = max( current_value[name + '_end_time' + keysuffix], value[name + '_end_time' + keysuffix] )
            current_value[name + '_start_time' + keysuffix] = min( current_value[name + '_start_time' + keysuffix], value[name + '_start_time' + keysuffix] )
            for key in value.keys():
              if not key.startswith(name) and key not in current_value.keys():
                current_value[key] = value[key]
            break
        else:
          history[client_id].append( dict(value) ) # makes a copy
      for value_list in [history[client_id]]:
        sort_values(value_list)
        needs_check = True
        while needs_check:
          needs_check = False
          if len( value_list ) > 1:
            last_value = value_list[0]
            for value in value_list[1:]:
              if last_value[name + '_end_time' + keysuffix] > value[name + '_start_time' + keysuffix]:
                # could happen if a client misses snaps
                if last_value[name + '_end_time' + keysuffix] > value[name + '_end_time' + keysuffix]:
                  # if no other piece is after, then need to split this piece
                  for next_value in value_list:
                    if next_value[name + '_start_time' + keysuffix] == value[name + '_end_time' + keysuffix]:
                      break
                  else:
                    # need another bit at the end then
                    new_value = dict(last_value)
                    new_value[name + '_start_time' + keysuffix] = value[name + '_end_time' + keysuffix]
                    value_list.append( new_value )
                    sort_values(value_list)
                    needs_check = True
                last_value[name + '_end_time' + keysuffix] = value[name + '_start_time' + keysuffix]
          for idx in range(len(value_list)):
            if idx >= len(value_list):
              break
            value = value_list[idx]
            while idx < len(value_list) - 1:
              next_value = value_list[idx + 1]
              if value[name + '_end_time' + keysuffix] == next_value[name + '_start_time' + keysuffix] and value[valuename] == next_value[valuename]:
                value[name + '_end_time' + keysuffix] = max( value[name + '_end_time' + keysuffix], next_value[name + '_end_time' + keysuffix] )
                del value_list[idx + 1]
              else:
                break
  return history

def merge_frags(datas, matchid):
  # rather than do a complicated merge w/ dedup, just pick own frags from each demo
  # and pick longest demo to use for missing frags
  frags = []
  ownfragclients = []
  longestmap = None
  longestmaptime = 0
  for data in datas:
    clientid = data['client']['id']
    mapidx, map = findmap(data['maps'], matchid)
    frags.extend( map['ownfrags'] )
    ownfragclients.append(clientid)
    maptime = map['map_end_time'] - map['map_start_time']
    if maptime > longestmaptime:
      longestmap = map
      longestmaptime = maptime
  frags.extend([frag for frag in longestmap['frags'] if frag['attacker'] not in ownfragclients])
  frags.sort( cmp = lambda x, y: x['time'] - y['time'] )
  return frags

def merge_histories(datas, matchid):
  return (merge_history(datas, matchid, 'name'), merge_history(datas, matchid, 'team'), merge_frags(datas, matchid))
