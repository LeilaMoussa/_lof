# \*lof
\<something\>lof: my undergrad research/capstone programming sandbox
----------------
## Todos

- [ ] test insert phase of ilof
- [ ] implement and test update phase of ilof
  - [ ] refactor SymPair and AsymPair
  - [ ] make changes to querykNN() to be able to use queryRkNN()
  - [ ] updateLrd
  - [ ] updateLof
- [ ] replace in-mem collections with state stores where possible
- [ ] return streams from certain operations (in maintain phase)
- [ ] implement memory ceiling for ilof (no summarization yet, just one deletion for each insertion)
- [ ] prototype lsh
  - [x] implement lsh using np
  - [ ] find lib for lsh
  - [ ] compare 3 results to each other and to flat index
- [ ] prototype hnsw
- [ ] maybe also ivf

...and then I can move on to all things summarization: dilof? milof? c_lof? but first, rlof!
