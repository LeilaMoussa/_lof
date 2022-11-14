# \*lof
\<something\>lof: my undergrad research/capstone programming sandbox
----------------
## Repository
- lof: replication of basic LOF
- anns: replications of ANNS techniques
- ilof: archived replication of ILOF
- rtlofs: multi-module project containing algorithms i'm aiming for
- producer.py: kafka producer
- roc.py: generates roc curve and calculates auc given expected and actual labeled data

## Immediate Todos

- [x] finish refactoring and debugging ILOF (cosmetic changes to be done last, e.g. logging and config defaults)
- [ ] label points as inliers or outliers, start with x%, then topN, then maybe, if i have time, using fixed threshold
- [ ] write results to sink file (3 kinds of results: full profiles, labeled data, just outliers)
- [ ] also write to sink topic (just outliers and labeled data)
- [ ] finish and test roc.py (python version problems here) (update: scikit example code is incorrect!)
- [x] call ilof from rlof
- [ ] finish rlof (note: EvictingQueue, TarsosLSH)
- [ ] generate ROC curve for RLOF
- [ ] C_LOF skeleton