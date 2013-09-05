import os
import json
import multiprocessing

from core.splitter import BaseSplitter, SmartLineSplitter
from core.mapper import BaseMapper
from core.reducer import BaseReducer

SPLITTER_CLASS = 'splitter_class'
MAPPER_CLASS = 'mapper_class'
REDUCER_CLASS = 'reducer_class'
MAPPER_NUM = 'mapper_num'
REDUCER_NUM = 'reducer_num'
INPUT_DIRS = 'input_dirs'
OUTPUT_PATH = 'output_path'

class Configure(object):
    def __init__(self, conf_file):
        self.conf_map = {
                SPLITTER_CLASS: SmartLineSplitter,
                MAPPER_CLASS: None,
                MAPPER_NUM: multiprocessing.cpu_count() + 1,
                REDUCER_CLASS: None,
                REDUCER_NUM: multiprocessing.cpu_count() + 1,
                INPUT_DIRS: None,
                OUTPUT_PATH: None,
                        }
        
        if not os.path.exists(conf_file):
            logging.error('can not find the configure file: %s' % conf_file)
            return None

        conf_dict = json.load(open(conf_file)):
        if SPLITTER_CLASS in conf_dict:
            self.conf_map[SPLITTER_CLASS] = conf_dict[SPLITTER_CLASS]
        if MAPPER_CLASS in conf_dict:
            self.conf_map[MAPPER_CLASS] = conf_dict[MAPPER_CLASS]
        if MAPPER_NUM in conf_dict:
            self.conf_map[MAPPER_NUM] = conf_dict[MAPPER_NUM]
        if REDUCER_CLASS in conf_dict:
            self.conf_map[REDUCER_CLASS] = conf_dict[REDUCER_CLASS]
        if REDUCER_NUM in conf_dict:
            self.conf_map[REDUCER_NUM] = conf_dict[REDUCER_NUM]
        if INPUT_DIRS in conf_dict:
            self.conf_map[INPUT_DIRS] = conf_dict[INPUT_DIRS]
        if OUTPUT_PATH in conf_dict:
            self.conf_map[OUTPUT_PATH] = conf_dict[OUTPUT_PATH]

    def __getitem__(self, key):
        if key not in self.conf_map:
            logging.warning('%s is not in the configure' % key)
            return None

        return self.conf_map[key]

class DefaultConfigure(Configure):
    def __init__(self):
        Configure.__init__(self, './conf/mapred.conf')

class Job(object):
    def __init__(self, conf):
        self.conf = conf
        # load default setting from configure
        self.splitter_class = conf[SPLITTER_CLASS]
        self.mapper_class = conf[MAPPER_CLASS]
        self.mapper_num = conf[MAPPER_NUM]
        self.reducer_class = conf[REDUCER_CLASS]
        self.reducer_num = conf[REDUCER_NUM]
        self.input_dirs = conf[INPUT_DIRS]
        self.output_path = conf[OUTPUT_PATH]

    def set_splitter(self, spliter):
        self.splitter = splitter

    def set_mapper(self, mapper_class):
        self.mapper_class = mapper_class

    def set_mapper_num(self, mapper_num):
        self.mapper_num = mapper_num

    def set_reducer_class(self, reducer_class):
        self.reducer_class = reducer_class

    def set_reducer_num(self, reducer_num):
        self.reducer_num = reducer_num

    def add_input_dir(self, input_dir):
        self.input_dirs.extends(input_dir)

    def set_output_path(self, output_path):
        self.output_path = output_path

    def _check_env(self):
        if not issubclass(self.splitter_class, BaseSplitter):
            logging.error('%s is not a subclass of BaseSpliter' % str(self.splitter_class))
            return False

        if not issubclass(self.mapper_class, BaseMapper):
            logging.error('%s is not a subclass of BaseMapper' % str(self.mapper_class))
            return False

        if not issubclass(self.reducer_class, BaseReducer):
            logging.error('%s is not a subclass of BaseReducer' % str(self.reducer_class))
            return False

        if type(self.mapper_num) is not int or self.mapper_num <= 0:
            logging.error('mapper number %s is invalid' % str(self.mapper_num))
            return False

        if type(self.reducer_num) is not int or self.reducer_num <= 0:
            logging.error('reducer number %s is invalide' % str(self.reducer_num))
            return False

        return True

    def run(self):
        if not self._check_env():
            logging.error('Job canceled since environment checking failed')
            return False

        return True                
