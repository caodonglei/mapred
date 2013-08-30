import os
import sys
import logging
import json

class BaseSplitter(object):
    '''
    A splitter partitions the input data into even parts which are distributed among multiple mappers.
    Note that the BaseSplitter cannot be initialized, since the _split() method is not implemented.
    '''
    def __init__(self, input, splitter_outputers, slice_num):
        self.input = input
        self.outputers = splitter_outputers
        self.slice_num = slice_num

    def _check_env(self):
        if self.input is None:
            logging.error("The input file/dir is none")
            return False
        elif not os.path.exists(self.input):
            logging.error("The input path %s is not exist" % str(self.input))
            return False

        if type(self.outputers) is list or len(self.outputers) != self.slice_num:
            logging.error("The splitter outputer is invalide with type=%s, length[%d] != %d." % \
                    (str(type(self.outputers)), 
                     0 if type(self.outputers) is not list else len(self.outputers), 
                     self.slice_num))
            return False

        return True

    def _format_kv(self, k, v):
        return '%s\n' % json.dumps({k, v})

    def _get_inputs(self):
        if os.path.isdir(self.input):
            for file in os.listdir(self.input):
               yield os.path.join(self.input, file)
        else:
            yield self.input

    def split(self):
        if not self._check_env():
            return False
        return self._split()

class LineSplitter(BaseSplitter):
    '''
    LineSplitter is a single thread file splitter.
    Each line of the input files is the value, while the line number is the key.
    '''
    def _split(self):
        line_count = 0
        idx = 0
        for file in self._get_inputs():
            logging.info("The LineSplitter is splitting file: %s" % str(file))
            for line in open(file):
                if idx >= self.slice_num: # do not use operator % which is computationally cost
                    idx = 0
                self.outputers[idx].write(self._format_kv(str(line_count), line[:-1]))
                line_count += 1
                idx += 1
        logging.info("The LineSplitter stops, there are %d lines." % line_count)
        return True

class LineSeperatorSplitter(BaseSplitter):
    '''
    The LineSeperatorSplitter performs the same as the LineSplitter mostly, except that you can specify the 
    seperator of each line and the index of the key as well as the index of the value which indicate the 
    index of the key and value of the splitted items by the given seperator respectively.
    '''
    def __init__(self, input, splitter_outputers, slice_num,
            seperator, key_idx, value_idx):
        BaseSplitter.__init__(self, input, splitter_outputers, slice_num)
        self.sperator = seperator
        self.key_idx = key_idx
        self.value_idx = value_idx

    def _check_env(self):
        if not BaseSplitter._check_env():
            return False

        if self.sperator is None:
            logging.error("The sepearator must not be none.")
            return False

        if type(self.key_idx) is not int or self.key_idx < 0:
            logging.error("The key index is invalid, type is %s, value is %d" % \
                    (str(type(self.key_idx)), -1 if type(self.key_idx) is not int else self.key_idx))
            return False

        if type(self.value_idx) is not int or self.value_idx < 0:
            logging.error("The key index is invalid, type is %s, value is %d" % \
                    (str(type(self.value_idx)), -1 if type(self.value_idx) is not int else self.value_idx))
            return False

        return True

    def _split(self):
        line_count = 0
        idx = 0
        for file in self._get_inputs():
            logging.info("The LineSplitter is splitting file: %s" % str(file))
            for line in open(file):
                parts = line[:-1].split(self.seperatoe)
                if self.key_idx > len(parts) or self.value_idx > len(parts):
                    logging.warning("The length of line: %s is less than key/value:%d/%d index." % \
                            (line[:-1], self.key_idx, self.value_idx))
                    continue

                if idx >= self.slice_num: # do not use operator % which is computationally cost
                    idx = 0
                self.outputers[idx].write(self._format_kv(parts[self.key_idx], parts[self.value_idx]))
                line_count += 1
                idx += 1
        logging.info("The LineSplitter stops, there are %d lines." % line_count)
        return True

class FileSplitter(BaseSplitter):
    '''
    The FileSplitter just renames the file names of the input dir, without the breaking of each file.
    The input of the FileSplitter must be a directory.
    '''
    def _check_env(self):
        if not BaseSplitter._check_env():
            return False

        if not os.path.isdir(self.input):
            return False

        return True

    def _split(self):
        pass

def test():
    test_data_path = './test/data'
    test_output = './test/output'
    line_splitter = LineSplitter(test_data_path, test_output, 4)

    seperator_splitter = LineSeperatorSplitter(test_data_path, test_output, 4, ' ', 0, 1)

    file_splitter = FileSplitter(test_data_path, test_output, 4)

if __name__ == '__main__':
    test()
