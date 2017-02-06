from sklearn import preprocessing

prop = lambda property: lambda frag: frag[property] if property in frag else 0

features = [
  prop('attacker_z_speed'),
  prop('attacker_xy_speed'),
  #prop('target_is_bot'),
  prop('missile_pitch'),
  prop('missile_lifetime'),
  prop('attacker_target_distance'),
  lambda frag: 1 if frag['attacker_team'] != frag['target_team'] else 0,
  prop('target_distance_last_second'),
  #prop('mod'),
  #prop('attacker_is_bot'),
  prop('target_had_flag'),
  prop('target_z_speed'),
  prop('attacker_distance_last_second'),
  prop('target_xy_speed'),
  prop('target_corpse_travel_z_distance'),
  prop('target_corpse_travel_distance')
]

modenc = preprocessing.OneHotEncoder(n_values=46)
modenc.fit([[x] for x in range(0,46)])

def make_example(frag, map):
  example = [float(feature(frag)) for feature in features]
  example.extend(modenc.transform([[frag['mod']]]).toarray()[0])
  return example
