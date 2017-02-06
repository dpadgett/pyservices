import trueskill
import sys
import json
import math

def win_probability(a, b):                                                      
    deltaMu = sum([x.mu for x in a]) - sum([x.mu for x in b])                   
    sumSigma = sum([x.sigma ** 2 for x in a]) + sum([x.sigma ** 2 for x in b])  
    playerCount = len(a) + len(b)
    if playerCount == 0:
      return 0.5
    denominator = math.sqrt(playerCount * (trueskill.BETA * trueskill.BETA) + sumSigma)             
    return trueskill.global_env().cdf(deltaMu / denominator)

if __name__ == '__main__':
  teams = {'R': [], 'B': []}
  key = '';
  for value in sys.argv[1:]:
    if value == 'R' or value == 'B':
      key = value
      continue
    if value == 'unknown':
      teams[key].append(trueskill.Rating())
      continue
    mu, sigma = value.split(',')
    teams[key].append(trueskill.Rating(mu=float(mu), sigma=float(sigma)))
  input = [teams['R'], teams['B']]
  #print teams
  #print trueskill.quality(input)
  result = {
    'red': {
      'win_probability': win_probability(teams['R'], teams['B'])
    },
    'blue': {
      'win_probability': win_probability(teams['B'], teams['R'])
    },
  }
  for team, key in [('red', 'R'), ('blue', 'B')]:
    if len(teams[key]) == 0:
      result[team]['rating'] = 0
    else:
      result[team]['rating'] = sum([x.mu for x in teams[key]]) / len(teams[key])
  # see which switch would make it the most fair
  bestprob = win_probability(teams['R'], teams['B'])
  bestswitch = []
  for redidx, redplayer in enumerate(teams['R']):
    for blueidx, blueplayer in enumerate(teams['B']):
      newred = list(teams['R'])
      newblue = list(teams['B'])
      newred[redidx] = blueplayer
      newblue[blueidx] = redplayer
      newprob = win_probability(newred, newblue)
      if abs(newprob - 0.5) < abs(bestprob - 0.5):
        bestswitch = [redidx, blueidx]
        bestprob = newprob
  result['switch'] = bestswitch
  result['switchprob'] = bestprob
  sys.stdout.write(json.dumps(result))
