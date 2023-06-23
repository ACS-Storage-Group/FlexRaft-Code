import subprocess
import sys

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Require two parameters: NIC and bandwidth")
        exit(1)
    nic = sys.argv[1]
    bandwidth = "1Gbit"
    cmd = "tc qdisc del dev {} root; tc qdisc add dev {} root handle 1:  htb default 11; tc class add dev {} parent 1: classid 1:11 htb rate {} ceil {}".format(
        nic, nic, nic, bandwidth, bandwidth)
    pr = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, shell=True)
    if pr.returncode != 0:
        print("[Limit Bandwidth Failed]")
    else:
        print("[Limit Bandwidth Succeed]")



