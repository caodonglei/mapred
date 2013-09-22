import os
import logging
import json
import sys
import time
import traceback

class CollectorConfigureError(Exception):
    def __init__(self, msg):
        self.value = msg

    def __str__(self):
        return repr(self.value)

class BaseCollector(object):
    '''
    Collector serializes the output of mapper or reducer,
    the output can be transfered across network or different
    processes.

    The parameter conf is a dict-like object which must
    contains the keys such as path, prefix and slice_num.
    The path is a local dir or socket address where the outputs
    will transfer.
    The prefix is the prefix of local file names or remote
    queue names to identify the output.
    The slice_num is the number of slices this collector
    should partition the outputs.
    '''
    def __init__(self, conf):
        self.conf = conf

    def _check_env(self):
        if self.conf is None or \
                'path' not in self.conf or \
                not self.conf['path'] or \
                'prefix' not in self.conf or \
                not self.conf['prefix'] or \
                'slice_num' not in self.conf or \
                type(self.conf['slice_num']) is not int or \
                not self.conf['slice_num'] > 0:
            logging.error('The conf of collector object is invalid: %s' % str(self.conf))
            return False
        self.slice_num = self.conf['slice_num'] # since slice_num is frequently used
        return True

    def collect(self, key, value):
        channel = hash(key) % self.conf['slice_num']
        self._collect(channel, key, value)

    def close(self):
        self._close()

class DebugCollector(BaseCollector):
    '''
    The DebugCollector prints the output to stdout
    '''
    def __init__(self):
        BaseCollector.__init__(self, {'path': 'stdout', 'prefix': 'stdout', 'slice_num': 1})

    def _collect(self, channel, key, value):
        sys.stdout.write('%s\t%s\n' % (key, value))

    def _close(self):
        pass

class FileCollector(BaseCollector):
    '''
    The FileCollector outputs the k/v to a local file.
    '''
    def __init__(self, conf):
        BaseCollector.__init__(self, conf)
        if not self._check_env():
            logging.error('The configure of FileCollector is invalid')
            raise CollectorConfigureError('The configure of FileCollector is invalid')

        if os.path.exists(self.conf['path']):
            if not os.path.isdir(self.conf['path']):
                logging.error('The path %s in the configure is not a diretory' % self.conf['path'])
                raise CollectorConfigureError('The path %s in the configure is not a directory' % self.conf['path'])
        else:
            os.mkdir(self.conf['path'])

        self.writers = [open(os.path.join(self.conf['path'], '%s_%d' % (self.conf['prefix'], i)), 'w') for i in range(self.conf['slice_num'])]

    def _collect(self, channel, key, value):
        if channel >= self.slice_num:
            channel = channel % self.slice_num
        self.writers[channel].write('%s\n' % json.dumps({key: value}))
        
    def _close(self):
        for writer in self.writers:
            writer.close()

class SocketCollector(BaseCollector):
    pass

from sorter import HeapSorter, HEAP_FULL
MAX_RESULTS_NUM = 'max_results_num'

class SortFileCollector(FileCollector):
    '''
    The SortFileCollector is like the FileCollector except
    that the outputs are partially sorted by keys.
    '''
    def __init__(self, conf):
        FileCollector.__init__(self, conf)
        self.heap_sorter = HeapSorter(conf['slice_num'])
        if MAX_RESULTS_NUM in conf:
            self.heap_sorter.set_max_result_num(conf[MAX_RESULTS_NUM])

    def _collect(self, channel, key, value):
        if channel >= self.slice_num:
            channel = channel % self.slice_num
        if HEAP_FULL == self.heap_sorter.add(channel, key, value):
            for (key, value) in self.heap_sorter.get_all_results(channel):
                self.writers[channel].write('%s\n' % json.dumps({key: value}))

    def _close(self):
        for idx, writer in enumerate(self.writers):
            for key, value in self.heap_sorter.get_all_results(idx):
                writer.write('%s\n' % json.dumps({key: value}))
            writer.close()

class SortSocketCollector(SocketCollector):
    pass

def test_sortfile_collector():
    # setup
    tmp_path = setup()

    # testing
    try:
        conf = {'path': tmp_path, 'prefix': 'test_file', 'slice_num': 3, MAX_RESULTS_NUM: 4}
        collector = SortFileCollector(conf)
        for i in range(20):
            collector.collect('key_%d' % (20-i), 'value_%d' % (20-i))
        collector.close()
    except:
        print traceback.format_exc()

    # tear down
    print conf
    tear_down(tmp_path)

def test_file_collector():
    # setup
    tmp_path = setup()

    # testing
    try:
        conf = {'path': tmp_path, 'prefix': 'test_file', 'slice_num': 3}
        collector = FileCollector(conf)
        for i in range(20):
            collector.collect('key_%d' % i, 'value_%d' % i)
        collector.close()
    except:
        print traceback.format_exc()

    # tear down
    print conf
    tear_down(tmp_path)

def setup():
    while True:
        tmp_path = 'tmp_%d' % int(time.time()) 
        if os.path.exists(tmp_path):
            continue
        os.mkdir(tmp_path)
        return tmp_path

def tear_down(tmp_path):
    for file in os.listdir(tmp_path):
        print '===%s==' % file
        file_path = os.path.join(tmp_path, file)
        for line in open(file_path):
            print '\t%s' % line[:-1]
        os.remove(file_path)
    os.rmdir(tmp_path)

def test_debug_collector():
    collector = DebugCollector()
    collector.collect('a', 'apple')
    collector.collect('b', 'banana')
    collector.close()

if __name__ == '__main__':
#    test_debug_collector()
#    test_file_collector()
    test_sortfile_collector()
