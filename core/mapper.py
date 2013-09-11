import logging
import inspect

from collector import BaseCollector, DefaultCollector

class MapConfigureError(Exception):
    def __init__(self, msg):
        self.value = msg

    def __str__(self):
        return repr(self.value)

class BaseMapper(object):
    def __init__(self, collector):
        self.collector = collector

    def _check_env(self):
        if self.map is None or \
                not inspect.ismethod(self.map):
            logging.error('The map function is none or is not method')
            return False

        map_signature = inspect.getargspec(self.map)
        if len(map_signature.args) == 4 and \
                map_signature.args[0] == 'self':
            pass
        elif len(map_signature.args) == 3 and \
                map_signature.args[0] != 'self':
            pass
        else:
            logging.error('The number of parameters[%d] is invalid' % \
                    len(map_signature.args))
            return False

        if self.collector is None or \
                not isinstance(self.collector, BaseCollector):
            logging.error('The collector is invalid.')
            return False;

        return self.collector._check_env()

    def set_inputs(self, inputs):
        self.inputs = inputs

    def run(self):
        if not self._check_env():
            raise MapConfigureError('The mapper environment is invalid.')

        for key, value in self.inputs:
            self.map(key, value, self.collector)

class MapperTemplate(BaseMapper):
    def __init__(self, map_func, collector):
        BaseMapper.__init__(self, collector)

        self.map_func = map_func
        if not self._check_env():
            logging.error('the map function is invalid')
            raise MapConfigureError("The map function is invalid.")

    def map(self, key, value, collector):
        self.map_func(key, value, collector)

def test():
    inputs = [('key', 'value')]
    dc = DefaultCollector()

    class MyMapper(BaseMapper):
        def map(self, key, value, collector):
            collector.collect(key, value)

    print 'test subclass of BaseMapper ......'
    bm = MyMapper(dc)
    bm.set_inputs(inputs)
    print 'expected: %s\t%s' % inputs[0]
    bm.run()

    def map_func(key, value, collector):
        collector.collect(key, value)

    print 'test MapperTemplate ......'
    bm = MapperTemplate(map_func, dc)
    bm.set_inputs(inputs)
    print 'expected: %s\t%s' % inputs[0]
    bm.run()

if __name__ == '__main__':
    test()
