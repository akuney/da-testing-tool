import numpy as np
import math
from collections import defaultdict
from numba import autojit,jit,double,void,int_

@jit('f8[:](f8[:],f8[:],int_,int_)')
def numba_get_raw_signature_cheap(vector,pool,num_hashes,dim):
    length = len(pool)
    res = np.zeros(num_hashes)
    for idx in range(num_hashes):
        val = 0.0
        tmp = 17*idx
        for i in range(dim):
            val += pool[(101*i+tmp) % length]*vector[i]        
        if val >= 0:
            res[idx] = 1.0
    return res


@jit('f8[:](int64[:],f8[:],f8[:],int_,int_)')
def numba_sparse_get_raw_signature_cheap(idx_vector,val_vector,pool,num_hashes,dim):
    N = len(idx_vector)
    length = len(pool)
    res = np.zeros(num_hashes)
    for hash_idx in range(num_hashes):
        val = 0.0
        tmp = 17*hash_idx
        for i in range(N):
            idx = idx_vector[i]
            val += pool[(101*idx+tmp) % length] * val_vector[i] # wait, does this actually work?
        if val >= 0:
            res[hash_idx] = 1.0
    return res

class DenseCosineLSHEncoder(object):
    def __init__(self,num_hashes,num_blocks,block_size,dim):
        self.num_hashes = num_hashes
        self.num_blocks = num_blocks
        self.block_size = block_size
        self.dim = dim
        self.random_matrix = np.random.randn(num_hashes, dim)
        self.hash_maps = [defaultdict(list) for k in xrange(self.num_blocks)]
        # experimental
        self.pool = np.random.randn(100000)
    
    def get_raw_signature(self,vector):
        # add simple matrix-vector multiplication here
        pass
    
    # slow, but we don't have to store the entire matrix of random projections
    def get_raw_signature_cheap(self,vector): 
        return numba_get_raw_signature_cheap(vector,self.pool,self.num_hashes,self.dim)
    
    def transform_signature(self,signature):
        # split into chunks of length block_size
        array_list = []
        for k in xrange(self.num_blocks):
            chunk = signature[k*(self.block_size):(k+1)*self.block_size]
            array_list.append(chunk)
        return array_list
    
    def get_sig_list(self,vector):
        raw_sig = self.get_raw_signature_cheap(vector)
        sig = raw_sig.astype(np.int64)
        sig_list = self.transform_signature(sig)
        output_list = []
        for sig in sig_list:
            bool_sig = np.maximum(sig,0)
            out = 0
            for bit in bool_sig:
                out = (out << 1) | bit
            output_list.append(out)
        return output_list

    def add_item(self,item,item_id):
        sig_list = self.get_sig_list(item)
        for sig,hash_map in zip(sig_list,self.hash_maps):
            hash_map[sig].append(item_id)

    def get_candidates(self,item):
        sig_list = self.get_sig_list(item)
        candidates = set() # ought to be a multiset?
        for sig,hash_map in zip(sig_list,self.hash_maps):
            candidates.update(hash_map[sig])
        return candidates

class SparseIDFCosineLSHEncoder(object):
    def __init__(self,num_hashes,num_blocks,block_size,dim):
        self.num_hashes = num_hashes
        self.num_blocks = num_blocks
        self.block_size = block_size
        self.dim = dim
        self.hash_maps = [defaultdict(list) for k in xrange(self.num_blocks)]
        # experimental
        self.pool = np.random.randn(100000)  # is this enough?
        
    def get_raw_signature(self,feature_dict,debugging=False):
        # I don't know how to make Numba handle dicts efficiently, so let's unwrap some shit
        #pairs = feature_dict.items()
        pairs = sorted(feature_dict.items(),key=lambda x:x[0])
        if debugging:
            print pairs
        idxs = np.array([k for k,v in pairs],dtype=np.int64)
        vals = np.array([v for k,v in pairs])
        return numba_sparse_get_raw_signature_cheap(idxs,vals,self.pool,self.num_hashes,self.dim)
    
    def transform_signature(self,signature):
        # split into chunks of length block_size
        array_list = []
        for k in xrange(self.num_blocks):
            chunk = signature[k*(self.block_size):(k+1)*self.block_size]
            array_list.append(chunk)
        return array_list
    
    # feature_dict is {idx->val}
    def get_sig_list(self,feature_dict):
        raw_sig = self.get_raw_signature(feature_dict)
        sig = raw_sig.astype(np.int64)
        sig_list = self.transform_signature(sig)
        output_list = []
        for sig in sig_list:
            bool_sig = np.maximum(sig,0)  # not sure this step is necessary any more?
            out = 0
            for bit in bool_sig:
                out = (out << 1) | bit
            output_list.append(out)
        return output_list
    
    def add_item(self,feature_dict,item_id):
        sig_list = self.get_sig_list(feature_dict)
        for sig,hash_map in zip(sig_list,self.hash_maps):
            hash_map[sig].append(item_id)
    
    def get_candidates(self,feature_dict):
        sig_list = self.get_sig_list(feature_dict)
        candidates = set() # ought to be a multiset?
        for sig,hash_map in zip(sig_list,self.hash_maps):
            candidates.update(hash_map[sig])
        return candidates
    
