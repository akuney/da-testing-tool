import sys
import csv
from collections import defaultdict
import numpy as np
import build_feature_val_counts as bf

NON_FONT_CHANGE_PERC = .20 # if we see two users A and B, the probability that some feature X has changed is 20%
FONT_CHANGE_PERC = .05
USER_DUPLICATION_PERC = .10

# should just make this python constant somewhere
FONTS = open("font_list.tsv").read().strip().split('\t')

def get_feature_counts():
    inf = open('feature_counts.tsv')
    count_map = bf.deserialize_count_map(inf)
    inf.close()
    return count_map

def decay_user(data_dict,font_list,sampler_map):
    # for each (non-font) feature, we want to choose some shit that changes
    new_data_dict = {}
    for feature_name in data_dict:
        if feature_name not in bf.FEATURES:
            # just copy
            new_data_dict[feature_name] = data_dict[feature_name]
        elif np.random.uniform() < NON_FONT_CHANGE_PERC:
            new_val = sampler_map[feature_name]()
            new_data_dict[feature_name] = new_val
        else:
            new_data_dict[feature_name] = data_dict[feature_name]
    
    # now handle fonts
    font_set = set(font_list)
    new_font_set = set()
    for font in font_list:
        if np.random.uniform() > FONT_CHANGE_PERC:
            new_font_set.add(font)
    
    for font in set(FONTS) - font_set:
        if np.random.uniform() < FONT_CHANGE_PERC:
            new_font_set.add(font)

    return new_data_dict,list(new_font_set)

if __name__ == '__main__':
    count_map = get_feature_counts()
    sampler_map = bf.build_sampler_from_count_map(count_map)

    font_probs = np.random.beta(size=len(FONTS),a=3,b=3)
    # each font has a popularity
    font_pops = dict(zip(FONTS,font_probs))

    inf = open(sys.argv[1])

    reader = csv.reader(inf,delimiter='\t')

    header = reader.next()
    
    sys.stdout.write('\t'.join(header) + '\t' + 'fonts' + '\n')
    for line in reader:
        data = dict(zip(header,line))
        samples = np.random.binomial(1,font_probs)
        current_fonts = sorted([font for k,font in enumerate(FONTS) if samples[k] > 0])

        current_line = [data[key] for key in header] + [','.join(current_fonts)]
        sys.stdout.write('\t'.join(current_line) + '\n')
        
        # create duplicate, if necessary
        if np.random.uniform() < USER_DUPLICATION_PERC:
            new_data,new_fonts = decay_user(data,current_fonts,sampler_map)
            # hack different id, so we can map back and forth
            new_data['id'] = data['id'] + '_A'
            new_fonts = sorted(new_fonts)
            new_line = [new_data[key] for key in header] + [','.join(new_fonts)]
            sys.stdout.write('\t'.join(new_line) + '\n')
        
