# FlexRaft: Minimizing Network and Storage Costs for Consensus with Flexible Erasure Coding

This repository contains the implementation of our *ICPP'23* paper: **Minimizing Network and Storage Costs for Consensus with Flexible Erasure Coding**. FlexRaft is a variant of the [Raft Consensus Algorithm](https://raft.github.io/) that minimizes the networking and storage costs using a flexible erasure coding scheme. More specifically, FlexRaft can detect the cluster's server failure case and dynamically change the erasure coding scheme to minimize the total volume of data transferred, consequently reducing the commit latency. 

## Building FlexRaft

### Prerequisites

* **Building System:** *cmake* version >= 3.8

* **Compiler:** *g++* version >= 10.2.1
* **Supported Platform:**  Our code is tested on Ubuntu20.04 and CentOS7. Operating systems of these platforms or of higher versions are supposed to support FlexRaft. 

**NOTE:** The above prerequisites can be satisfied by using devtoolset-10 on CentOS7. 

### Dependencies

FlexRaft requires a bunch of dependence third-party libraries, including [RocksDB](https://github.com/facebook/rocksdb), [Intel isa-l](https://github.com/intel/isa-l). Execute the following command to install all dependencies. Ensure the host can access the network and the user has root privilege. 

```bash
python3 scripts/install_dependencies.py
```

### Build

After successfully installing all dependencies, run the following command to build the codebase, which includes the library, unit tests and benchmarking tools. 

```bash
CMAKE=cmake make build
```

---

## Running FlexRaft examples

The FlexRaft codebase provides codes as examples and benchmarking tools in the ``bench`` directory, after building the codebase, execute the following steps to start the raft cluster servers and a client. 

* **Prepare the cluster configuration** file in the following format:

  ```bash
  <node id> <ip:raft_port> <ip:kv_port> <log> <db>
  ```

  * **node id** (ingeter): The identifier of a raft server. 
  *  **ip:port**: The ip address of a raft server and associated ports for raft communication and KV service communication. 
  * **log**: The path to the location of the raft log.
  * **db**: The RocksDB path used as a state machine.

  Here we give an example configuration file that describes a cluster comprising 3 servers :

  ```bash
  # example.conf
  0 172.20.126.134:50001 172.20.126.134:50002 /tmp/raft_log0 /tmp/testdb0
  1 172.20.126.135:50001 172.20.126.135:50002 /tmp/raft_log1 /tmp/testdb1
  2 172.20.126.136:50001 172.20.126.136:50002 /tmp/raft_log2 /tmp/testdb2
  ```

* **Start up the raft servers** by executing the following command on each server:

  ```bash
  cd build
  bench/bench_server --conf=<conf> --id=<id>
  ```

  * ``--conf``: specify the path to the configuration file created in the first step. 
  * ``--id``: specify the identifier of the raft server that is running. 

  For example, given the configuration file created in step1, one can start the raft server on ``172.20.126.134`` using the following command: 

  ```bash
  cd build
  bench/bench_server --conf=example.conf --id=0
  ```

* **Start the client** by executing the following command on a dedicated server (different from raft servers in step 2). This step runs a client that continuously sends *Put* request to the raft server. 

  ```
  cd build
  bench/bench_client --conf=<conf> --id=0 --size=<size> --write_num=<write_num>
  ```

  * ``--conf``: specify the path to the configuration file. 
  * ``--id``: The identifier of this client, this parameter is useless and can be omitted. 
  * ``--size``: specify the size of each key-value pair. The format is like "4K", "2M".
  * ``--write_num``: specify the number of *Put* requests to send.

  For example, one can start up a client that issues 10000 *Put* requests, each of which contains 2MiB payloads, to the raft cluster in the following command:

  ```bash
  cd build
  bench/bench_server --conf=example.conf --id=0 --size=2M --write_num=10000
  ```

* **Results:** The client will print the execution results, including the average latency of each *Put* operation, the raft commit latency and the state machine apply latency. 

  ```bash
  # example client execution results
  [Results][Succ Cnt=1000][Average Latency = 60431 us][Average Commit Latency = 36181 us][Average Apply Latency = 3473]
  ```
