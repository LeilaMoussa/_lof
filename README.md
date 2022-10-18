# \*lof
\<something\>lof: my undergrad research/capstone programming sandbox
----------------
## Todos

- [x] finish insert phase of ilof
- [ ] finish update phase of ilof
  - [x] refactor SymPair and AsymPair
  - [x] make changes to querykNN() to be able to use queryRkNN()
  - [x] updateLrd
  - [x] updateLof
  - [ ] clear disposable collections
- [ ] finish aggregation of ilof
  - [ ] use avro and/or confluence classes for serde
- [ ] test ilof
  - [x] populate topic
  - [ ] debug indexing and map keying errors -> make sure all points run all the way through
  - [ ] inspect mouse-outliers-topic
  - [x] write simple producer
  - [ ] then sink outliers into file and compare file with correct answers (py?)
- [ ] refactor ilof
- [ ] config
- [ ] replace in-mem collections with state stores where possible (?)
- [ ] return streams from certain operations (in maintain phase) (?)
- [ ] implement memory ceiling for ilof (think about how to approach this, i.e. particular implementation)
- [ ] prototype lsh
  - [x] implement lsh using np
  - [ ] find lib for lsh
  - [ ] compare 3 results to each other and to flat index (?)
- [ ] prototype hnsw
- [ ] maybe also ivf
