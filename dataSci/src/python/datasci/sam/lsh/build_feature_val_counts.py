import sys
import csv
from collections import defaultdict
import numpy as np
import random

FEATURES = ['ip_address','browser_version','screen_char','locId','city','postalCode','os']

def build_count_map(reader):
    header = reader.next()
    counts = {}

    for line in reader:
        data = dict(zip(header,line))
        for key in FEATURES:
            if key in counts:
                counts[key][data[key]] += 1
            else:
                counts[key] = defaultdict(int)
                counts[key][data[key]] += 1
    
    for feature_name in counts:
        d = counts[feature_name]
        s = float(sum(d.values()))
        for feature_val in d:
            d[feature_val] /= s

    return counts

def serialize_count_map(count_map,outf):
    for feature_name in count_map:
        d = count_map[feature_name]
        for feature_val in d:
            s = str(feature_name) + '\t' + str(feature_val) + '\t' + str(d[feature_val])
            outf.write(s + '\n')

def deserialize_count_map(inf):
    reader = csv.reader(inf,delimiter='\t')
    count_map = {}
    for feature_name,feature_val,perc in reader:
        if feature_name in count_map:
            count_map[feature_name][feature_val] = float(perc)
        else:
            count_map[feature_name] = {feature_val:float(perc)}
    return count_map


# ought to write binary search version of this
def weighted_choice_sub(weights):
    rnd = random.random() * sum(weights)
    for i, w in enumerate(weights):
        rnd -= w
        if rnd < 0:
            return i

# map of feature names to weighted sampler
def build_sampler_from_count_map(count_map):
    d = {}
    def make_sampler(feature_name):
        items = count_map[feature_name].items()
        percs = np.array([x[1] for x in items])
        feature_vals = [x[0] for x in items]
        #return lambda: np.random.choice(feature_vals,p=percs)
        #return lambda: feature_vals[weighted_choice_sub(percs)]
        # weighted sampling is too damn slow
        return lambda: random.choice(feature_vals)
    
    for key in count_map:
        d[key] = make_sampler(key)
    
    return d

if __name__ == '__main__':
    inf = open(sys.argv[1])
    reader = csv.reader(inf,delimiter='\t')    
    count_map = build_count_map(reader)
    inf.close()
    
    outf = open('feature_counts.tsv','w')
    serialize_count_map(count_map,outf)
    outf.close()
    
