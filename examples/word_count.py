import sys

from core.job import Job, DefaultConfigure
from core.splitter import LineSplitter
from core.mapper import BaseMapper
from core.reducer import BaseReducer

class WordCountMapper(BaseMapper):
    def map(self, key, value, collector):
        words = value.split()
        for word in words:
            collecter.collect(word, 1)

class WordCountReducer(BaseReducer):
    def reduce(self, key, values, collector):
        count = len(values)
        collector.collect(key, count)

def main():
    if len(sys.argv) < 3:
        print 'usage: %s input_dir output_dir' % sys.argv[0]
        return
        
    conf = DefaultConfigure()
    job = Job(conf)
    job.set_mapper(WordCountMapper)
    job.set_mapper_num(4)
    job.set_reducer(WordCountReducer)
    job.set_reducer_num(1)
    
    job.add_input_path(sys.argv[1])
    job.set_output_path(sys.argv[2])
    
    print job.run()

if __name__ == '__main__':
    main()
