import sys
import random
import math

NUM_INNER_ITERS = 100
NUM_OUTER_ITERS = 1000
NUM_STRATEGY_ITERS = 250
NUM_SIMULATION_ITERS = 100
NUM_CANDIDATE_BIDS = 100
TOLERANCE = .01

def mean(x):
    return sum(x)/float(len(x))

def uniform(n,lo,hi):
    return [random.uniform(lo,hi) for _ in range(n)]

def unlist(l):
    return [x for y in l for x in y]

def argmax(array):
    best_idx = 0
    best_val = array[0]
    for k,x in enumerate(array):
        if x >= best_val:  # makes plots look nicer
            best_val = x
            best_idx = k
    return best_idx

class Auction(object):
    def __init__(self, num_slots, num_entrants, decay, min_bid):
        self.num_slots = min(num_slots,num_entrants)
        #self.num_slots = num_slots
        self.num_entrants = num_entrants
        self.min_bid = min_bid
        self.decay = decay
        self.ctrs = [math.pow(decay, k) for k in range(self.num_slots)]
        
    def draw_bids_for_others(self, current_strategy, num_draws):
        N = self.num_entrants - 1
        bid_landscapes = []
        for _ in range(num_draws):
            valuations = uniform(N, 0, 1)
            bids = [current_strategy*x for x in valuations]
            processed_bids = [x for x in sorted(bids, reverse=True) if x >= self.min_bid]
            bid_landscapes.append(processed_bids)
        return bid_landscapes
    
    # maybe use inverse distribution function instead
    def get_candidate_bids(self, our_valuation):
        return [k*our_valuation/NUM_CANDIDATE_BIDS for k in range(NUM_CANDIDATE_BIDS)]
        
    def evaluate_single_auction(self, our_valuation, our_bid, bid_landscape):
        """ returns payout.  bid_landscape must be sorted in descending order """
        if our_bid < self.min_bid:
            return 0.0
        
        num_effective_competitors = len(bid_landscape)
        for k in range(min(num_effective_competitors,self.num_slots)):
            if our_bid >= bid_landscape[k]:
                return self.ctrs[k] * (our_valuation - bid_landscape[k])
        
        if self.num_slots > num_effective_competitors:
            return self.ctrs[num_effective_competitors] * (our_valuation - self.min_bid)

        return 0.0
    
    def draw_valuation(self, current_strategy):
        our_valuation = uniform(1,0,1)[0]
        bid_landscapes = self.draw_bids_for_others(current_strategy, NUM_INNER_ITERS)
        candidate_bids = self.get_candidate_bids(our_valuation)
        
        candidate_payouts = [0.0] * len(candidate_bids)
        for k,candidate_bid in enumerate(candidate_bids):
            sample_payouts = [self.evaluate_single_auction(our_valuation, candidate_bid, bid_landscape) for bid_landscape in bid_landscapes]
            candidate_payouts[k] = mean(sample_payouts)
        
        idx = argmax(candidate_payouts)
        return our_valuation, candidate_bids[idx], candidate_payouts[idx]
    
    def draw_many_valuations(self, current_strategy, num_draws):
        """ returns list of triples of valuation, bid, payout """
        return [self.draw_valuation(current_strategy) for _ in range(num_draws)]
    
    def fit_linear_strategy(self, payout_dataset):
        bids = [x[1] for x in payout_dataset if x[0] >= self.min_bid]
        valuations = [x[0] for x in payout_dataset if x[0] >= self.min_bid]
        return sum(bids)/sum(valuations)

    
    def equilibrate(self):
        current_strategy = 1.0 # start at truth-telling
        strategy_list = [current_strategy]
        for k in range(NUM_STRATEGY_ITERS):
            payout_dataset = self.draw_many_valuations(current_strategy, NUM_OUTER_ITERS)
            new_strategy = self.fit_linear_strategy(payout_dataset)
            print str(k) + "," + str(new_strategy)
            
            if abs(current_strategy-new_strategy) < TOLERANCE:
                return new_strategy
            
            current_strategy = new_strategy
            strategy_list.append(current_strategy)

        return mean(strategy_list[-10:])

    

if __name__ == '__main__':
    auction = Auction(5, 5, .7, .01)
    
    # examine best response to truth-telling
    # payout_dataset = auction.draw_many_valuations(1.0, 10000)
    # for valuation, bid, payout in payout_dataset:
    #    print str(bid) + "," + str(valuation) + "," + str(payout)

    #find Bayes-Nash equilibrium, possibly
    print auction.equilibrate()
