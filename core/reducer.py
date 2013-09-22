import logging
import inspect

from collector import BaseCollector, DebugCollector

class ReduceConfigureError(Exception):
    def __init__(self, msg):
        self.value = msg

    def __str__(self):
        return repr(self.value)

class BaseReducer(object):
    def __init__(self, collector):
        self.collector = collector

    def _check_env(self):
        if self.reduce is None or \
                not inspect.ismethod(self.reduce):
            logging.error('The reducer function is none or is not method')
            return False

        reduce_signature = inspect.getargspec(self.reduce)
        if len(reduce_signature.args) == 4 and \
                reduce_signature.args[0] == 'self':
            pass
        elif len(reduce_signature.args) == 3 and \
                reduce_signature.args[0] != 'self':
            pass
        else:
            logging.error('The number of parameters[%d] is invalid' % \
                    len(reduce_signature.args))
            return False

        if self.collector is None or \
                not isinstance(self.collector, BaseCollector):
            logging.error('The collector is invalid.')
            return False;

        return self.collector._check_env()

    def set_input_dicts(self, input_dicts):
        self.input_dicts = input_dicts

    def run(self):
        if not self._check_env():
            raise ReduceConfigureError('The reducer environment is invalid.')

        for key, values in self.input_dicts.items():
            self.reduce(key, values, self.collector)

class ReducerTemplate(BaseReducer):
    def __init__(self, reduce_func, collector):
        BaseReducer.__init__(self, collector)

        self.reduce_func = reduce_func
        if not self._check_env():
            logging.error('the reduce function is invalid')
            raise ReduceConfigureError("The reduce function is invalid.")

    def reduce(self, key, values, collector):
        self.reduce_func(key, values, collector)

def test():
    inputs = {'key': ['value1', 'value2']}
    dc = DebugCollector()

    class MyReducer(BaseReducer):
        def reduce(self, key, values, collector):
            for value in values:
                collector.collect(key, value)

    print 'test subclass of BaseMapper ......'
    bm = MyReducer(dc)
    bm.set_input_dicts(inputs)
    print 'expected: %s' % ''.join(('%s\t%s' % (key, str(values)) for key, values in inputs.items()))
    bm.run()

    def reduce_func(key, values, collector):
        for value in values:
            collector.collect(key, value)

    print 'test ReducerTemplate ......'
    bm = ReducerTemplate(reduce_func, dc)
    bm.set_input_dicts(inputs)
    print 'expected: %s' % ''.join(('%s\t%s' % (key, str(values)) for key, values in inputs.items()))
    bm.run()

if __name__ == '__main__':
    test()
