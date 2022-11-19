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
- [x] finish and test roc.py (python version problems here)
- [x] call ilof from rlof
- [ ] finish rlof.java
    - [x] create all collections
    - [x] pass and/or import them in and out of ilof
    - [x] age-based deletion
        - [x] points
        - [x] black holes
    - [x] get multiple black holes the point belongs to and update all of them, including radius
    - [ ] decide on how ilof treats virtual points (in progress, pretty messy)
    - [x] average reachdist logic
- [ ] plug TarsosLSH into ILOF
    - [x] fix folder structure
    - [ ] look at radius business
- [ ] generate ROC curve for RLOF
- [ ] C_LOF skeleton