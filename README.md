﻿# HadoopKPrototypesClustering

K-prototypes of mixed types with arbitrary dimensions using Hadoop MapReduce

## CommandLine usage

```
hadoop jar kprototypes-0.0.1.jar -conf [configuration file] [HdfsInputPath] [HdfsOuputPath] [numberOfClusters] [maxIterationTime]
```

* HdfsInputPath - hdfs path to the input file
* HdfsOutputPath - hdfs path for the output
* numberOfClusters - namely, k
* maxIterationTime - max iteration times for the k-prototypes

## Example

```
hadoop jar kprototypes-0.0.1.jar -conf conf/kprototypesSetting.xml /input /kpoutput 3 3 
```
