import sys
import csv
from collections import defaultdict
import numpy as np
import Vectorizers as V
import LSHEncoders as lsh

MAX_LINES = 10000000
FEATURES = ['ip_address','browser_version','screen_char','locId','city','postalCode','os']

class Input(object):
    def __init__(self,filename,delimiter='\t',max_lines=-1):
        self.filename = filename
        self.delimiter = delimiter
        self.inf = open(self.filename)
        self.reader = csv.reader(self.inf,delimiter=self.delimiter)
        self.max_lines = max_lines
        self.count_interval = 1000
        self.count = 0
        
        self.header = self.reader.next()

    def __iter__(self):
        return self

    def next(self):
        self.count += 1
        if self.count % self.count_interval == 0:
            print self.count
        if self.count >= self.max_lines and self.max_lines > 0:
            raise StopIteration
        
        return self.reader.next()
        
    def close(self):
        self.inf.close()

    def reset(self):
        self.inf.close()
        self.inf = open(self.filename)
        self.reader = csv.reader(self.inf,delimiter=self.delimiter)
        _ = self.reader.next()
        self.count = 0

def process_data(header,line):
    data = dict(zip(header,line))
    fonts = data['fonts'].strip().split(',')
    item = [data[key] for key in FEATURES] + fonts
    return item,data['id']

def build_vectorizer(input):
    vectorizer = V.BasicVectorizer()
    header = input.header
    count = 0
    tmp_store = {}
    lines = {}
    
    for line in input:
        item,item_id = process_data(header,line)
        vectorizer.add_item(item,item_id)
    
    return vectorizer

def build_encoder(input,vectorizer,num_hashes,num_blocks,block_size):
    header = input.header
    tmp_store = {}
    lines = {}

    encoder = lsh.SparseIDFCosineLSHEncoder(num_hashes,num_blocks,block_size,10000) # last doesn't really matter
    for line in input:
        item,item_id = process_data(header,line)
        feature_dict = vectorizer.get_idf_feature_dict(item)        
        encoder.add_item(feature_dict,item_id)
        
        # TAKE ME OUT WHEN RUNNING ON FULL DATASET
        #tmp_store[item_id] = feature_dict
        #lines[item_id] = item
    return encoder

def evaluate_encoder(input,vectorizer,encoder):
    fn,tp,pos = 0,0,0
    total_length = 0
    count = 0
    for line in input:
        count += 1
        item,item_id = process_data(input.header,line)
        feature_dict = vectorizer.get_idf_feature_dict(item)
        candidates = encoder.get_candidates(feature_dict)
        candidates.remove(item_id) # hopefully item_id is always in the candidate set...
        total_length += len(candidates)
        
        if item_id.endswith('_A'): # denotes added user
            pos += 1
            matching_item_id = item_id.strip('_A')
            if matching_item_id in candidates:
                tp += 1
            else:
                fn += 1
    
    print fn,tp,pos,float(total_length)/count

if __name__ == '__main__':
    num_hashes = 750
    num_blocks = 50
    block_size = 15
    
    input = Input(sys.argv[1],'\t',MAX_LINES)
    vectorizer = build_vectorizer(input)
    input.reset()
    print "done vectorizing"
    encoder = build_encoder(input,vectorizer,num_hashes,num_blocks,block_size)
    input.reset()
    print "done encoding"
    evaluate_encoder(input,vectorizer,encoder)
    

            
