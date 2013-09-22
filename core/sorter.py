import heapq
import json

DEFAULT_MAX_RESULTS_NUM = 10000

HEAP_NORMAL = 0
HEAP_FULL = 1

class HeapSorter(object):
    def __init__(self, slice_num):
        self.slice_num = slice_num
        self.max_results_num = DEFAULT_MAX_RESULTS_NUM
        self.local_results = [[] for i in range(slice_num)]

    def set_max_result_num(self, max_res_num):
        '''
        The max_results_num controls the number of k/v pairs
        stored in the local_results. Since large number of
        k/v pairs consumes large amount of memory, no element
        should be added in case that the number of existing
        k/v pairs achieves the max_results_num.
        '''
        if max_res_num < self.max_results_num:
            self.max_results_num = max_res_num

    def add(self, index, k, v):
        heapq.heappush(self.local_results[index], (k, v))
        if len(self.local_results[index]) >= self.max_results_num:
            return HEAP_FULL
        return HEAP_NORMAL

    def get_all_results(self, index):
        for i in xrange(len(self.local_results[index])):
            yield heapq.heappop(self.local_results[index])

class ConcurrentMergeSorter(object):
    '''
    The ConcurrentMergeSorter merges all the lists at the
    same time. Each time the smallest element of all the
    first elements of each list is moved to a final list.
    This kind of sorter is useful if the number of elements
    is small.
    '''
    pass

class SequentMergeSorter(object):
    '''
    The SequentMergeSorter each time generates a merged list from two
    sorted lists by merge sorting. As a result, the number 
    of lists left to be merged is reduced by one each time, 
    util there is only one list containing all the elements
    of original lists is left. If there is a large number
    of elements need to be merged, this kind of sorter should
    be used.
    '''
    pass
