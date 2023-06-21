import util
import sys
import time
import subprocess
import re
from random import randrange

class Results:
    def __init__(self, bw):
        # bw in Mbps
        self.bw = bw

def parse_result(r:str) -> Results:
    thpt = re.findall(r'Throughput: (\d+\.\d+)', r)
    return Results(sum([float(a) for a in thpt]))

def run_bench(N:int, f:int, bench_type:str, values:str, op_count:int):
    print("\nRun benchmark: N={} f={} type = {} values={} op_count={}".format(N, f, bench_type, values, op_count))
    cfg_file = "cluster_{}.conf".format(N)
    servers = util.ParseClusterConfiguration(cfg_file, "-i ~/.ssh/FlexibleK_Experiment.pem")

    # Kick out some servers
    for i in range(f):
        servers.pop(randrange(len(servers)))

    # bootstrap these raft servers
    bin = "/root/FlexRaft-Code/build/bench/ycsb_server"
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
    client_bin = "/root/FlexRaft-Code/build/bench/ycsb_client"
    client_cfg = "/root/FlexRaft-Code/exp/{}".format(cfg_file)

    client_cmd = "{} --conf={} --client_num=4 --size={} --op_count={} --type={}".format(
        client_bin, client_cfg, values, op_count, bench_type)
    pr = subprocess.run(client_cmd, stdout=subprocess.PIPE, shell=True)
    result = Results(0)
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
    value_size = "2048K"
    op_count  = 5000
    bench_types = ["YCSB_A", "YCSB_B", "YCSB_C", "YCSB_D", "YCSB_F"]

    results = []

    for i in range(len(bench_types)):
        results.append(run_bench(N, f, bench_types[i], value_size, op_count))

    # output the results
    for i in range(len(bench_types)):
        print("[Bench: {}][Throughput: {:.2f} Mbps]".format(bench_types[i], results[i].bw))
