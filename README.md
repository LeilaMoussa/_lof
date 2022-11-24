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

- [ ] process expected labeled data sets
    - [ ] KDDCup99 10%
- [ ] debug RLOF sink file
- [ ] generate ROC curve for RLOF
    - [ ] with flat index
    - [ ] with lsh
    - [ ] with varying k
    - [ ] with varying W
    - [ ] with varying MAX_AGE
- [ ] calculate execution times
    - [ ] ILOF
    - [ ] RLOF with flat index
    - [ ] RLOF with lsh
- [ ] verify VP distances check out
- [ ] benchmark accuracy of TarsosLSH in a vacuum
    - [ ] vary HASHES
    - [ ] vary HASHTABLES
- [ ] write some kind of results to sink topic
- [ ] make producer.py read line by line
- [ ] would be nice to have: customize sink file format or add that logic to roc.py
