import util
import sys

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Require at least three arguments")
        exit(1)
    N = int(sys.argv[1])
    nic = sys.argv[2]
    cfg_file = "cluster_{}.conf".format(N)
    servers = util.ParseClusterConfiguration(cfg_file, "-i ~/.ssh/FlexibleK_Experiment.pem")

    for server in servers:
        server.limit_bw(nic, "1Gbit")
