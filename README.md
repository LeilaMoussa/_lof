# \*lof
\<something\>lof: Undergrad research turned capstone project
----------------
## Overview
- [lof](https://github.com/LeilaMoussa/_lof/tree/master/lof): naïve replication of basic LOF from Breunig, M. M., Kriegel, H. P., Ng, R. T., & Sander, J. (2000, May). LOF: identifying density-based local outliers. In Proceedings of the 2000 ACM SIGMOD international conference on Management of data (pp. 93-104).
- [anns](https://github.com/LeilaMoussa/_lof/tree/master/anns): replications of ANNS techniques, so far just LSH with random projection, though I would have liked to also explore NSW and HNSW. See https://www.pinecone.io/learn/vector-indexes/ for more
- [ilof](https://github.com/LeilaMoussa/_lof/tree/master/ilof): **archived** replication attempt of ILOF from Pokrajac, D., Lazarevic, A., & Latecki, L. J. (2007, March). Incremental local outlier detection for data streams. In 2007 IEEE symposium on computational intelligence and data mining (pp. 504-515). IEEE.
- [rtlofs](https://github.com/LeilaMoussa/_lof/tree/master/rtlofs): multi-module project containing algorithms i'm aiming for, now contains ILOF and RLOF + datasets + sink files + miscellaneous ROC curves. Dependency in lsh/ module from: https://github.com/JorenSix/TarsosLSH
- [producer.py](https://github.com/LeilaMoussa/_lof/blob/master/producer.py): kafka producer
- [roc.py](https://github.com/LeilaMoussa/_lof/blob/master/roc.py): generates roc curve and calculates auc given expected and actual labeled data
- [key-dataset.py](https://github.com/LeilaMoussa/_lof/blob/master/key-dataset.py): utility to add keys to a dataset file
- [label-kdd.py](https://github.com/LeilaMoussa/_lof/blob/master/label-kdd.py): utility to sample and downsize KDDCUP99 dataset
- [choose-param.py](https://github.com/LeilaMoussa/_lof/blob/master/choose-param.py): utility to compute and graph roc auc and time elapsed from sink files while varying a single parameter. Assumes sink files are named as expected on capstone.common.Utils.buildSinkFilename().

## Background

http://www.aui.ma/sse-capstone-repository/pdf/fall-2022/Density%20Guided%20Optimized%20Real%20Time%20Outlier%20Detection.pdf

## Usage

To run rtlofs (assuming Kafka is running and the source topic exists):

`mvn clean package && java -cp <path-to-fatjar-of-Driver> capstone.Driver`

To enable assertions, add `-ea`.

It will output the time elapsed in stdout and a sink file for the labels (inlier, outlier) whose name contains the parameters used. Kill with ^C.

## Contributors

What to do next, in order:

0 - You might want to verify the correctness of code. Synthesize small and simple test cases whose outcomes you can predict and inspect the output of RLOF.

1 - Automate the benchmarking process. Extend the code or write a script that a) takes as input the parameter(s) to vary and their ranges, b) feeds these parameters to the rtlofs project (by writing to .env for example, or running Driver.main() using command line arguments), c) uses the output to calculate ROC AUC and PR AUC, d) keeps track of heap allocation and usage and time elapsed (Java utilities), and e) reports on these 3 dimensions of performance using automatically generated graphs for example. The idea is to control the runtime environment and to allow you to walk away from the computer while experiments run on different datasets with different parameters, for you to come back in the morning to nice graphs and logs. The challenge is that, at the time of writing, the Kafka Streams application expects some given topic to be filled by another application, currently a Python producer, and the Kafka Streams app is stopped with a keyboard interrupt, so automating those two aspects is not straightforward. Redesign may be necessary.

2 - After gathering enough reliable results about RLOF's performance, you may want to a) continue finetuning RLOF or b) start implementing other algorithms to compare it to. Option a: If speed is the problem, find algorithmic bottlenecks (like linear searches or summarization overhead) and use smarter techniques. You might want to try to find parallelization opportunities! If memory is the problem, try to smartly decrease the amount of collections we're allocating, or trade off processing for memory. If accuracy is the problem, RLOF may need to be reconceptualized. Find TODO and IMPROVE comments in the code and start with some of those. Option b: I recommend you do as many of the following algorithms in order: C_LOF, MiLOF, DILOF. In one semester, you might only be able to fully implement one of these. I recommend a test driven approach and a great focus on correctness from the start.

Simpler contributions like implementing more ANNS techniques and more distance measures are also very welcome, as well as any code improvements like testing, optimization, style, good practices, etc. Please feel free to email me at leila.farah.moussa@gmail.com for absolutely any questions.
