# FlexRaft: Minimizing Network and Storage Costs for Consensus with Flexible Erasure Coding

This repository contains the implementation of our *ICPP'23* paper: **Minimizing Network and Storage Costs for Consensus with Flexible Erasure Coding**. FlexRaft is a variant of the [Raft Consensus Algorithm](https://raft.github.io/) that minimizes the networking and storage costs using a flexible erasure coding scheme. More specifically, FlexRaft can detect the cluster's server failure case and dynamically change the erasure coding scheme to minimize the total volume of data transferred, consequently reducing the commit latency. 

## Build

FlexRaft requires a bunch of dependence third-party libraries, including RocksDB, ISA-L. Execute the following command to install all dependencies. Note that the user must ensure the host can access the network and python3 is installed. 

```bash
python3 scripts/install_dependencies.py
```

After successfully installing all dependencies, run the following command to build the codebase, which includes the library, unit tests and benchmarking tools. 

```bash
CMAKE=cmake make build
```

---

