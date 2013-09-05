import logging
import inspect

class BaseMapper(object):
    def _check_env(self):
        if self.map is None:
            logging.error('The map function is none')
            return False

        map_signature = inspect.getargspec(self.map)
        if len(map_signature.args) == 4 and map_signature.args[0] == 'self':
            return True 
        elif len(map_signature.args) == 3 and map_signature.args[0] != 'self':
            return True
        else:
            logging.error('The number of parameters[%d] is invalid' % len(map_signature.args))
            return False

    def map(self, key, value, collector):
        pass

class MapperTemplate(BaseMapper):
    def __init__(self, map_func):
        BaseMapper.__init__(self)
        self.map = map_func
        if not self._check_env():
            logging.error('the map function is invalid')
            self.map = None

def test():
    bm = BaseMapper()
    class MyMapper(BaseMapper):
        def map(self, key, value, collector):
            pass

if __name__ == '__main__':
    test()
