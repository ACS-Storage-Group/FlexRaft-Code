# Reproduce  Experiment Results

This folder contains the script to reproduce our experimental results shown in the paper. Please refer to [this page](../README.md) for basic information of this code base. 

## Building

The following steps illustrate building the FlexRaft codebase on a CentOS-7 system. 

```bash
sudo su root
cd /root/
git clone git@github.com:ACS-Storage-Group/FlexRaft-Code.git
cd FlexRaft-Code
python3 scripts/install_dependencies.py
CMAKE=cmake make build
```

After these steps, you will get a ``build`` directory containing the generated binary files. The benchmarking binary files are placed in ``build/bench``, including:

* ``bench_server/bench_client``: Benchmarking for write performance.
* ``ycsb_server/ycsb_client``: Benchmarking for YCSB performance
* ``readbench_client``: Benchmarking for read performance. 

### Note

* Ensure the system has ***cmake* version >= 3.8** and ***g++* >= 10.2.1**. For CentOS-7 users, one can use the Developer Toolset:

  ```
  yum install -y devtoolset-10
  scl enable devtoolset-10 bash
  ```

  Then execute the building command illustrated above. 

* Ensure the user has root privilege so that the third-party libraries can be installed. 

* Add the installed path of ``libisal`` and ``rocksdb`` to the environmental variable ``LD_LIBRARY_PATH``. Otherwise, the building process will fail in the linking phase. 

---

## Reproducing

### Scripts

This folder contains three scripts to reproduce the experimental results, each of which corresponds to a class of experiments:

| script             | category                     | experiments         |
| ------------------ | ---------------------------- | ------------------- |
| ``write_bench.py`` | write performance benchmarks | Exp#1-Exp#4, Exp#11 |
| ``ycsb_bench.py``  | YCSB performance benchmarks  | Exp#5               |
| ``read_bench.py``  | Read performance benchmarks  | Exp#9               |

The scripts may need **minor modifications** to adapt to the benchmarking environment. We now detail these modifications as follows:

* **Prepare configuration file:** The user may need to create the configuration files (in the ``exp`` directory) according to the cluster configuration. The cluster configuration file has the same format as mentioned in [Running FlexRaft Examples](../README.md). The user should name the configuration files to be ``"cluster_N.conf"`` where ``N`` corresponds to the server number in this cluster. For example, we give an example of ``cluster_5.conf`` as follows:

  ```bash
  # cluster_5.conf
  0 172.20.126.134:50001 172.20.126.134:50002 /tmp/raft_log0 /tmp/testdb0
  1 172.20.126.135:50001 172.20.126.135:50002 /tmp/raft_log1 /tmp/testdb1
  2 172.20.126.136:50001 172.20.126.136:50002 /tmp/raft_log2 /tmp/testdb2
  3 172.20.126.137:50001 172.20.126.137:50002 /tmp/raft_log3 /tmp/testdb3
  4 172.20.126.138:50001 172.20.126.138:50002 /tmp/raft_log4 /tmp/testdb4
  ```

* **SSH Connection:** The user may need to change the second parameter of ``util.ParseClusterConfiguration`` in the three scripts:

  ```python
  # write_bench.py, ycsb_bench.py, read_bench.py
  servers = util.ParseClusterConfiguration(cfg_file, "-i ~/.ssh/FlexibleK_Experiment.pem")
  ```

  We specify the parameter to be ``"-i ~/.ssh/FlexibleK_Experiment.pem"`` in the scripts because we need this token file to establish SSH connection in our test environment. The user may set it to be an empty string. 

* **Change binary file location:**  For the ``run_bench`` function of these three scripts， the user may need to specify the absolute path of the binary files located on each raft server:

  ```python
  # bootstrap these raft servers
  bin = "/root/FlexRaft-Code/build/bench/bench_server"
  cfg = "/root/FlexRaft-Code/exp/{}".format(cfg_file)
  ```

  If the repository is located at ``/root/``, then the user doesn't need any changes. 

---

### Run the scripts

Run the following scripts step by step in the client-server. 

* **Limit the network bandwidth**

  ```bash
  python3 limit_bandwidth <N> <nic>
  python3 ../scripts/limit_bandwidth <nic> 1Gbit
  ```

  ``<N>`` corresponds to the number of servers in the raft cluster, before running this script, make sure the corresponding configuration file has been created in this folder. 

  ``<nic>``  corresponds to the NIC where the IP in the configuration file is associated.  

  For example, for a cluster containing five servers, each of which uses ``eth0`` for communication. Run the above commands: 

  ```
  python3 limit_bandwidth 5 eth0
  python3 ../scripts/limit_bandwidth eth0 1Gbit
  ```

  can set the network bandwidth of these servers to be 1Gbit/s. 

* **Write performance benchmarks**

   ```
   python3 write_bench.py <N> <f>
   ```

  This script requires two parameters ``N`` and ``f``, which correspond to the total number of servers and the number of failed servers in this cluster respectively. For example, running ``python write_bench.py 5 1`` means running write benchmarks in a cluster of 5 servers, where one of them is a failed server. The user should create the corresponding ``cluster_N.conf`` configuration file in advance (see above).

  Using this script together with different ``N`` and ``f`` parameters can generate our experimental results of Exp#1-Exp#4 and Exp#11: 

  ```bash
  # Exp#1
  python3 write_bench.py 5 0 
  
  # Exp#2
  python3 write_bench.py 7 0
  
  # Exp#3
  python3 write_bench.py 5 1
  
  # Exp#4
  python3 write_bench.py 7 1
  python3 write_bench.py 7 2
  
  # Exp#11
  python3 write_bench.py 9 1
  python3 write_bench.py 9 2
  python3 write_bench.py 9 3
  python3 write_bench.py 11 2
  python3 write_bench.py 11 3
  python3 write_bench.py 11 4
  ```

* **YCSB Performance Benchmark**

  ```
  python3 ycsb_bench.py <N> <f>
  ```

  This script reproduces the results of YCSB benchmarks, where ``N`` and ``f`` have the same meaning as in the **write performance benchmark** section. 

  ```bash
  # Exp#5
  python3 ycsb_bench.py 7 1
  python3 ycsb_bench.py 7 2
  ```

* **Read Performance Benchmark**

  ```bash
  python3 read_bench.py <N>
  ```

  This script reproduces the results of Read benchmarks. ``N`` is the only required parameter to specify the total number of servers in the raft cluster. 

  ```bash
  # Exp#11
  python3 read_bench.py 7
  ```

  

