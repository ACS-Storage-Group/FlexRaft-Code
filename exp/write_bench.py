import util
import sys
from random import randrange


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Require at least three arguments")
        exit(1)
    N = int(sys.argv[1])
    f = int(sys.argv[2])
    cfg_file = "cluster_{}.conf".format(N)
    servers = util.ParseClusterConfiguration(cfg_file, "")

    for i in range(f):
        servers.pop(randrange(len(servers)))

    # randomly choose a few servers and not bootstrap them
    for server in servers:
        print("id = {}, ip = {}, log = {}, sm = {}".format(
            server.id, server.ip, server.log, server.sm))

    print("[Parse Configuration File successfully...]")

    # bootstrap these raft servers
    bin = "/root/FlexRaft-Code/build/bench/bench_server"
    cfg = "/root/FlexRaft-Code/exp/{}".format(cfg_file)

    for server in servers:
        pr = server.bootstrap_as_server(bin, cfg)
        if pr != 0:
            print("[Bootstrap Server{} failed]".format(server.id))
            exit(1)

    print("[BootStrap all Raft servers successfull...]")

    



