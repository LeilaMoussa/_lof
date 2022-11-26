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

- [x] fix comparator bug in RLOF
- [ ] process expected labeled data sets
    - [x] (toy dataset: mouse)
    - [x] KDDCup99 10%
    - [ ] pendigit?
    - [ ] vowel?
    - [ ] shuttle?
- [x] debug RLOF sink file
- [x] generate ROC curve for ILOF (satisfactory)
- [x] generate ROC curve for RLOF
    - [ ] with varying index
    - [ ] with varying k
    - [ ] with varying W
    - [ ] with varying MAX_AGE
    - [ ] with varying INLIER_PERCENTAGE
- [ ] calculate execution times (remember to comment out / disable stuff like logging/printing/asserting)
    - [ ] ILOF
    - [ ] RLOF with flat index
    - [ ] RLOF with lsh
- [x] verify VP distances check out (they don't quite but needs further investigation)
- [x] benchmark accuracy of TarsosLSH in a vacuum (done with random vectors)
- [ ] write some kind of results to sink topic
- [x] make producer.py read line by line
- [ ] would be nice to have: customize sink file format or add that logic to roc.py
- [x] write as many "tests" (they're not really) as possible (WIP) -- write stuff to verify LSH!
