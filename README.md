A Hanborq optimized Hadoop Distribution, especially with high performance of MapReduce. It's the core part of HDH (Hanborq Distribution with Hadoop for Big Data Engineering).   

# HDH (Hanborq Distribution with Hadoop)   
Hanborq, a start-up team focuses on Cloud & BigData products and businesses, delivers a series of software products for Big Data Engineering, including a optimized Hadoop Distribution.   

HDH delivers a series of improvements on Hadoop Core, and Hadoop-based tools and applications for putting Hadoop to work solving Big Data problems in production. HDH is ideal for enterprises seeking an integrated, fast, simple, and robust Hadoop Distribution. In particular, if you think your MapReduce jobs are slow and low performing, the HDH may be you choice.    

## Hanborq optimized Hadoop   
It is a open source distribution, to make Hadoop **Fast**, **Simple** and **Robust**.    
**- Fast:** High performance, fast MapReduce job execution, low latency.   
**- Simple:** Easy to use and develop BigData applications on Hadoop.   
**- Robust:** Make hadoop more stable.   

## MapReduce Benchmarks  
The Testbed: 5 node cluster (4 slaves), 8 map slots and 1 reduce slots per node.  

**1. MapReduce Framework Job Latency**  
bin/hadoop jar hadoop-examples-0.20.2-hdh3u2.jar sleep -m 32 -r 4 -mt 1 -rt 1   
bin/hadoop jar hadoop-examples-0.20.2-hdh3u2.jar sleep -m 96 -r 4 -mt 1 -rt 1   
![HDH MapReduce Framework Job Latency](https://github.com/hanborq/hadoop/wiki/images/hdh-mapreduce-framework-job-latency.jpg)  
In order to reduce job latency, HDH implements **Distributed Worker Pool** like Google Tenzing. HDH MapReduce framework does not spawn new JVM processes for each job/task, but instead keep the slot processes running constantly. 
Additionally, there are many other improvements at this aspect.  

**2. MapReduce Processing Model Performance**    
**3. Terasort**  
Please refer to the page [MapReduce Benchmarks](https://github.com/hanborq/hadoop/wiki/MapReduce-Benchmarks) for detail.  

## Features   
### MapReduce   
**- Fast job launching:** such as the time of job lunching drop from 15 seconds to 1 second.   
**- Low latency:** not only job setup, job cleanup, but also data shuffle, etc.   
**- High performance shuffle:** low overhead of CPU, network, memory, disk, etc.   
**- Sort avoidance:** some case of jobs need not sorting, which result in too many unnecessary system overhead and long latency.   

... and more and continuous ...   

## Compatibility   
The API, configuration, scripts are all back-compatible with [Apache Hadoop](http://hadoop.apache.org/) and [Cloudera Hadoop(CDH)](http://www.cloudera.com/hadoop/). The user and developer need not to study new, except new features.

## Innovations and Inspirations   
The open source community and our real enterprise businesses are the strong source of our continuous innovations. 
Google, the great father of MapReduce, GFS, etc., always outputs papers and experiences that bring us inspirations, such as:   
[MapReduce: Simplified Data Processing on Large Clusters](http://research.google.com/archive/mapreduce.html)   
[MapReduce: A Flexible Data Processing Tool](http://cacm.acm.org/magazines/2010/1/55744-mapreduce-a-flexible-data-processing-tool)   
[Tenzing: A SQL Implementation On The MapReduce Framework](http://research.google.com/pubs/pub37200.html)   
[Dremel: Interactive Analysis of Web-Scale Datasets](http://research.google.com/pubs/pub36632.html)   

... and more and more ...   

## Open Source License   
All Hanborq offered code is licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0). And others follow the original license announcement.   
