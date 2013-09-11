import os
import logging
import json
import sys

class BaseCollector(object):
    '''
    Collector serializes the output of mapper or reducer,
    the output can be transfered across network or different
    processes.
    '''
    def __init__(self, conf):
        self.conf = conf

    def _check_env(self):
        return True

    def collect(self, key, value):
        self._collect(key, value)

class DefaultCollector(BaseCollector):
    '''
    The DefaultCollector prints the output to stdout
    '''
    def __init__(self):
        BaseCollector.__init__(self, None)

    def _collect(self, key, value):
        sys.stdout.write('%s\t%s\n' % (key, value))

class FileCollector(BaseCollector):
    pass
    
class SocketCollector(BaseCollector):
    pass
