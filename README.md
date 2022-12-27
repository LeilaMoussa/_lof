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

## Immediate Todos and Improvements
- [x] Implement LSH from scratch, no more TarsosLSH
- [ ] Use KD Tree for kNN search in ILOF instead of flat index (wip)
- [ ] Write even more tests in the form of assertions
- [ ] Automate benchmarking and evaluation
- [ ] Add precision-recall in addition to ROC/AUC

## Later Todos
- [ ] Clean up repository from unused/insignificant files
- [ ] Expressive code comments
- [ ] Incremental LSH
- [ ] Parallelization opportunities

## Contributions
- See various meta-comments in code prefixed with TODO, IMPROVE, BUG, etc.
- See any of the TODOs above
- C_LOF
- MiLOF
- Perhaps other algorithms
- More ANNS techniques
- More distance measures
- Derive .env parameterss from dataset
- Any code improvements: testing, optimization, style, good practices, etc.

