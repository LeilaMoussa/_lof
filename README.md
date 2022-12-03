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

- [x] vp getDistance with predefined distances
- [ ] measure speed and accuracy for varitions i've decided on (wip)
- [ ] after speed and accuracy, re-run same algs and watch visualvm, report on memory if possible (update: just get max heap usage in jprofiler)
- [ ] get both roc and precision recall curves (couldn't figure out scikit's stuff)
- [x] make roc compare labels key by key instead of simply sorting

## Later Todos
- [ ] write more tests in the form of assertions
- [ ] nice code comments
- [ ] credit lof, ilof, and tarsoslsh authors
- [ ] credit rlof authors (us)
- [ ] make a nice readme and contributor's guide
