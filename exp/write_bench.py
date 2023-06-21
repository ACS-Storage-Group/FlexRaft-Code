import util
import sys
import time
import subprocess
import re
from random import randrange

class Results:
    def __init__(self, write_latency, commit_latency, apply_latency):
        self.write_latency = write_latency
        self.commit_latency = commit_latency
        self.apply_latency = apply_latency

def parse_result(r:str) -> Results:
    write_lat_pattern = r"Average Latency = (\d+) us"
    commit_lat_pattern = r"Average Commit Latency = (\d+) us"
    apply_lat_pattern = r"Average Apply Latency = (\d+) us"

    write_lat = re.findall(write_lat_pattern, r)[0]
    commit_lat = re.findall(commit_lat_pattern, r)[0]
    apply_lat = re.findall(apply_lat_pattern, r)[0]

    return Results(write_lat, commit_lat, apply_lat)

def run_bench(N:int, f:int, values:str, write_count:int):
    print("Run benchmark: N={} f={} values={} write_count={}".format(N, f, values, write_count))
    cfg_file = "cluster_{}.conf".format(N)
    servers = util.ParseClusterConfiguration(cfg_file, "-i ~/.ssh/FlexibleK_Experiment.pem")

    # Kick out some servers
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

    print("[BootStrap all Raft servers successfull...]")

    time.sleep(1)

    # bootstrap the client
    client_bin = "/root/FlexRaft-Code/build/bench/bench_client"
    client_cfg = "/root/FlexRaft-Code/exp/{}".format(cfg_file)

    client_cmd = "{} --conf={} --id=0 --size={} --write_num={}".format(
        client_bin, client_cfg, values[i], write_count[i])
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
    if len(sys.argv) < 3:
        print("Require at least three arguments")
        exit(1)
    N = int(sys.argv[1])
    f = int(sys.argv[2])

    # bootstrap current server as a client
    values = ["4K", "16K", "64K", "128K", "256K", "512K", "1024K", "2048K"]
    write_count = [1000] * len(values)

    results = []

    for i in range(len(values)):
        results.append(run_bench(N, f, values[i], write_count[i]))

    # output the results
    for i in range(len(values)):
        print("[Value Size: {}][Write Latency: {} us]".format(values[i], results[i].write_latency))



