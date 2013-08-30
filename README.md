mapred
======

A map/reduce-based computing framework implemented by Python using multiprocessing. 
The first version only facilicates from the multi-cores of a single machine. New
version will take multiple machines into accound by delivering messages among machines
across the network.

The procedure of a map/reduce job is as follows:

masive input data -> splitter -> data partitions -> mapper(sorter) -> shuffler -> reducer -> collector -> output data
