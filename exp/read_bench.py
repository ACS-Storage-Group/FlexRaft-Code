import util
import sys
import time 
import subprocess
import re
from random import randrange

class Results:
    def __init__(self, fast_read, recover_read, repeated_read):
        self.fast_read = fast_read
        self.recover_read = recover_read
        self.repeated_read = repeated_read

def parse_result(r:str) -> Results:
    avg_lat = re.findall(r"Average Latency = (\d+) us", r)[0]
    recover_lat = re.findall(r"Recover Read Latency = (\d+) us", r)[0]
    fast_lat = re.findall(r"Fast Read Latency = (\d+) us", r)[0]

    return Results(fast_lat, recover_lat, avg_lat)

def run_bench(N:int, f:int, value_size:str, write_count:int, repeated:int):
    print("\nRun readbench: N={} f={} value_size={} write_count={} repeated={}".format(
        N, f, value_size, write_count, repeated))
    cfg_file = "cluster_{}.conf".format(N)
    servers = util.ParseClusterConfiguration(cfg_file, "-i ~/.ssh/FlexibleK_Experiment.pem")

    # Kick out some servers randomly
    for i in range(f):
        servers.pop(randrange(len(servers)))

    # bootstrap these raft servers
    bin = "/root/FlexRaft-Code/build/bench/bench_server"
    cfg = "/root/FlexRaft-Code/exp/{}".format(cfg_file)

    # bootstrap servers
    for server in servers:
        pr = server.bootstrap_as_server(bin, cfg)
        if pr != 0:
            print("[Bootstrap Server{} failed]".format(server.id))
            exit(1)

    print("[BootStrap all Raft servers successfully...]")

    time.sleep(1)

    # bootstrap the client
    client_bin = "/root/FlexRaft-Code/build/bench/readbench_client"
    client_cfg = "/root/FlexRaft-Code/exp/{}".format(cfg_file)

    client_cmd = "{} --conf={} --id=0 --size={} --write_num={} --repeated={}".format(
        client_bin, client_cfg, value_size, write_count, repeated)
    pr = subprocess.run(client_cmd, stdout=subprocess.PIPE, shell=True)
    result = Results(0, 0, 0)
    if pr.returncode != 0:
        print("Execute client failed")
    else:
        result = parse_result(str(pr.stdout))

    for server in servers:
        server.clear()

    return result

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Require at least three arguments")
        exit(1)
    N = int(sys.argv[1])
    repeated_read = 10
    F = (N - 1) // 2;

    # bootstrap current server as a client
    failures = [0, 1]
    write_count = 1000;
    values = "2048K"
    results = []

    for i in range(len(failures)):
        results.append(run_bench(N, failures[i], values, write_count, repeated_read))

    # output the results
    for i in range(len(failures)):
        k = N - failures[i] - F
        m = N - k
        print("[({}, {})][Raft: {} us][FlexRaft(1): {} us][FlexRaft(10) Latency: {} us]".format(
            k, m, results[i].fast_read, results[i].recover_read, results[i].repeated_read))
