# \*lof
\<something\>lof: Undergrad research turned capstone project
----------------
## Overview
- [https://github.com/LeilaMoussa/_lof/tree/master/lof](lof): naïve replication of basic LOF from Breunig, M. M., Kriegel, H. P., Ng, R. T., & Sander, J. (2000, May). LOF: identifying density-based local outliers. In Proceedings of the 2000 ACM SIGMOD international conference on Management of data (pp. 93-104).
- [https://github.com/LeilaMoussa/_lof/tree/master/anns](anns): replications of ANNS techniques, so far just LSH with random projection, though I would have liked to also explore NSW and HNSW. See https://www.pinecone.io/learn/vector-indexes/ for more
- [https://github.com/LeilaMoussa/_lof/tree/master/ilof](ilof): **archived** replication attempt of ILOF from Pokrajac, D., Lazarevic, A., & Latecki, L. J. (2007, March). Incremental local outlier detection for data streams. In 2007 IEEE symposium on computational intelligence and data mining (pp. 504-515). IEEE.
- [https://github.com/LeilaMoussa/_lof/tree/master/rtlofs](rtlofs): multi-module project containing algorithms i'm aiming for, now contains ILOF and RLOF + datasets + sink files + miscellaneous ROC curves. Dependency in lsh/ module from: https://github.com/JorenSix/TarsosLSH
- [https://github.com/LeilaMoussa/_lof/blob/master/producer.py](producer.py): kafka producer
- [https://github.com/LeilaMoussa/_lof/blob/master/roc.py](roc.py): generates roc curve and calculates auc given expected and actual labeled data
- [https://github.com/LeilaMoussa/_lof/blob/master/key-dataset.py](key-dataset.py): utility to add keys to a dataset file
- [https://github.com/LeilaMoussa/_lof/blob/master/label-kdd.py](label-kdd.py): utility to sample and downsize KDDCUP99 dataset

## Immediate Todos and Improvements
- [ ] Implement LSH from scratch, no more TarsosLSH
- [ ] Use KD Tree for kNN search in ILOF instead of flat index
- [ ] Write more tests in the form of assertions
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
- Any code improvements: testing, style, good practices, etc.
