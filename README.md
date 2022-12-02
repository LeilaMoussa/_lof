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

- [ ] vp getDistance with predefined distances
- [ ] measure
    - [ ] ilof flat
    - [ ] ilof lsh
    - [ ] rlof (always implies lsh + summarization + age deletion)
        - vary lsh
            - [ ] hashes=4, hashtables=4 (default)
            - [ ] hashes=1, hashtables=1
        - vary summarization
            - [ ] W = 10%, W=25%, W=60%
            - [ ] I=5%, I=10%, I=20%
- [ ] get both roc and precision recall curves
