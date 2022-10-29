# \*lof
\<something\>lof: my undergrad research/capstone programming sandbox
----------------
## Todos

- [x] finish insert phase of ilof
- [x] finish update phase of ilof
  - [x] refactor SymPair and AsymPair
  - [x] make changes to querykNN() to be able to use queryRkNN()
  - [x] updateLrd
  - [x] updateLof
  - [x] clear disposable collections
- [ ] finish aggregation of ilof (?)
  - [ ] use avro and/or confluence classes for serde (?)
- [ ] test ilof
  - [x] populate topic
  - [x] debug indexing and map keying errors -> make sure all points run all the way through
  - [ ] debug INFINITY bug
  - [ ] inspect mouse-outliers-topic
  - [x] write simple producer
  - [ ] then sink outliers into file and compare file with correct answers (py?)
  - [ ] producer.py: read line by line
  - [ ] test with other files
- [ ] refactor ilof
- [ ] config
- [ ] implement memory ceiling for ilof (EvictingQueue?)
- [ ] prototype lsh
  - [x] implement lsh using np
  - [x] find lib for lsh
  - [ ] compare 3 results to each other and to flat index (?)
- [ ] prototype hnsw and laybe also ivf (?)
