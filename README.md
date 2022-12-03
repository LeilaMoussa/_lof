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
- [ ] measure speed and accuracy for
    - [ ] ilof flat
    - [ ] ilof lsh
    - [ ] rlof (always implies lsh + summarization + age deletion)
        - vary lsh
            - [ ] hashes=4, hashtables=4 (default)
            - [ ] hashes=1, hashtables=1
        - vary summarization
            - [ ] W = 10%, W=25%, W=60%
            - [ ] I=5%, I=10%, I=20%
- [ ] after speed and accuracy, re-run same algs and watch visualvm, report on memory if possible
- [ ] get both roc and precision recall curves
- [ ] make roc compare labels key by key instead of simply sorting

## Later Todos
- [ ] write more tests in the form of assertions
- [ ] nice code comments
- [ ] credit lof, ilof, and tarsoslsh authors
- [ ] credit rlof authors (us)
- [ ] make a nice readme and contributor's guide
