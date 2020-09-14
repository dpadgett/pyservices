from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
import re
from sklearn import preprocessing, svm
import json

from make_example import make_example

db = MongoClient("mongodb").demos
demodb = db.mindemos
matchdb = db.minmatches
#matches = list(matchdb.find())#{'is_match': True}))
prefixes = ['cyd/perpetuality_of_nervouism', 'ent/psychodelic']
#prefixes = ['ent/psychodelic']
alldemos = [{'id': demo['_id']} for demo in demodb.find({'_id': {'$regex': '|'.join(['^%s.*' % prefix for prefix in prefixes])}}, {})]
examples = []
classes = []
exampleids = []

for demo in alldemos:
  print 'Processing', demo['id']
  #demodata = demodb.find({'_id':demo['id']})[0]
  with open('/cygdrive/U/demos/' + demo['id'] + '.dm_meta', 'r') as f:
    demodata = {'metadata': json.loads(f.read())}
  map = demodata['metadata']['maps'][0]
  for amap in demodata['metadata']['maps']:
    #print 'map duration: %d' % (amap['map_end_time'] - amap['map_start_time'])
    if amap['map_end_time'] - amap['map_start_time'] > map['map_end_time'] - map['map_start_time']:
      map = amap
  name = re.sub(r"\([^)]*\)", '', re.sub(r"\.dm_26", '', re.sub(r".*/", '', demo['id'])))
  rawtimes = re.split(',|_|-', name)
  times = []
  for time in rawtimes:
    if (len(time) != 4):
      time = time[:4]
      #print 'invalid time', time
      #continue
    try:
      times.append((int(time[0:2]) * 60 + int(time[2:4])) * 1000 + 3000) # add extra 3s due to low precision of timestamp
    except ValueError:
      print 'couldn\'t parse time', time
      continue
  if len(times) == 0:
    print 'found no good times, skipping'
    continue
  times = list(reversed(times))
  nexttimeidx = 0
  print 'found good times', str(['%02d:%02d' % (time/60000, (time%60000)/1000) for time in times])
  frags = map['ownfrags']
  # since all the timestamps should be after the frag, search in reverse order
  numgoodfrags = 0
  numbadfrags = 0
  for frag in reversed(frags):
    exampleids.append([demo['id'], frag])
    nexttime = 0
    if nexttimeidx < len(times):
      nexttime = times[nexttimeidx]
    fragtime = int(frag['time'])
    if fragtime < nexttime:
      while nexttimeidx < len(times) and fragtime < times[nexttimeidx]:
        nexttimeidx += 1
      nexttime = times[nexttimeidx - 1]
      delta = nexttime - fragtime
      if delta < 9000:
        examples.append(make_example(frag, map))
        classes.append(2) # 2 = good
        numgoodfrags += 1
        continue
      else:
        print 'passed time', nexttime, 'but delta was', delta
    examples.append(make_example(frag, map))
    classes.append(1) # 1 = bad
    numbadfrags += 1
  print 'found', numgoodfrags, 'good frags and', numbadfrags, 'bad frags'

train_end = int(len(examples) * 0.9)
scaler = preprocessing.StandardScaler().fit(examples)
scaledexamples = scaler.transform(examples)
clf = svm.SVR(kernel='rbf') #, probability=True)
clf.fit(scaledexamples[0:train_end], classes[0:train_end])
#results = [clf.predict(example.reshape(1,-1))[0] for example in scaledexamples]
results = clf.predict(scaledexamples)
difference = [i if results[i] > 1.5 and classes[i] == 1 else 0 for i in range(0,len(results))]
values = [d for d in difference if d != 0]
print [[results[i], i] for i in values]

from joblib import dump, load
dump(clf, 'niceshot.joblib')
dump(scaler, 'niceshotscaler.joblib')

#from sklearn.neural_network import MLPRegressor
#regr = MLPRegressor(hidden_layer_sizes=(40,), random_state=1, max_iter=500, verbose=True).fit(scaledexamples[0:train_end], classes[0:train_end])

def precision_recall(threshold):
  start = train_end
  true_positives = len([1 for i in range(start,len(results)) if results[i] > threshold and classes[i] == 2])
  false_positives = len([1 for i in range(start,len(results)) if results[i] > threshold and classes[i] == 1])
  true_negatives = len([1 for i in range(start,len(results)) if results[i] < threshold and classes[i] == 1])
  false_negatives = len([1 for i in range(start,len(results)) if results[i] < threshold and classes[i] == 2])
  precision = true_positives * 1.0 / (true_positives + false_positives)
  recall = true_positives * 1.0 / (true_positives + false_negatives)
  return [precision, recall]
