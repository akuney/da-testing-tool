import numpy as np
import math

class BasicVectorizer(object):
    def __init__(self,store_items=False):
        self.store_items = store_items
        self.forward_dict = {}
        self.backward_dict = {}
        self.item_map = {}
        self.feature_counts = {}
        self.num_features = 0
        self.total_items = 0
        
    # do we even need to store the dudes we've already seen?
    # let's just treat items as fucking iterables
    def add_item(self,item,item_id=None):
        self.total_items += 1.0
        
        item_id = item['id'] if item_id is None else item_id
        features = np.zeros(len(item),dtype=np.int64) # I think this helps out Numba?

        if self.store_items:
            self.item_map[item_id] = features
        
        for k,feature in enumerate(item):
            if feature in self.forward_dict:
                idx = self.forward_dict[feature]
                features[k] = idx
            else:
                self.forward_dict[feature] = self.num_features
                features[k] = self.num_features
                self.num_features += 1
            
            if feature in self.feature_counts:
                self.feature_counts[feature] += 1
            else:
                self.feature_counts[feature] = 1

    # item is a list of features in raw form, not indexes
    # returns dict of indexes to idf'd values
    def get_idf_feature_dict(self,item):
        d = {}
        for feature in item:
            if feature in self.feature_counts:
                count = self.feature_counts[feature]
            else:
                count = 0
            
            if feature in self.forward_dict:
                idx = self.forward_dict[feature]
                d[idx] = math.log10(self.total_items / (1.0+float(count)))
            else:
                print "haven't seen " + feature
        
        return d
