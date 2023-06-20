import subprocess

class Server:
    def __init__(self, id, ip, port, uname, passwd, log, sm, ssh_token) -> None:
        self.id = id
        self.ip = ip
        self.port = port
        self.uname = uname
        self.passwd = passwd
        self.log = log
        self.sm = sm
        self.ssh_token = ssh_token

    def execute(self, cmd:str):
        ssh_cmd = "ssh {} {}@{}".format(self.ssh_token, self.uname, self.ip) + " \"" + cmd + "\""
        pr = subprocess.run(ssh_cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, shell=True)
        if pr.returncode != 0:
            print("[Server({}) execute command {} failed, please retry]".format(self.id, cmd))
            return pr.returncode
        else:
            print("[Server({}) execute command {} succeed]".format(self.id, cmd))
            return 0

    def shutdown(self):
        cmd = "killall bench_server; killall bench_client; killall ycsb_server; killall ycsb_client"
        return self.execute(cmd)

    def clear(self):
        self.shutdown()
        cmd = "rm -rf {}; rm -rf {}".format(self.log, self.sm)
        return self.execute(cmd)

    def bootstrap_as_server(self, bin:str, conf:str):
        cmd = "nohup {} --conf={} --id={} > /dev/null 2&>1 &".format(bin, conf, self.id)
        return self.execute(cmd)


def ParseClusterConfiguration(conf_file:str, ssh_token:str) -> [Server]:
    servers = []
    f = open(conf_file, "r")
    lines = f.readlines()

    for line in lines:
        tokens = line.strip('\n').split(" ")
        id = int(tokens[0])
        ip = tokens[1].split(':')[0]
        log = tokens[-2]
        sm = tokens[-1]
        servers.append(Server(id, ip, "22", "root", "", sm, log, ssh_token))

    f.close()
    return servers

def ClearTestContext(servers: [Server], token:str):
    for server in servers:
        server.clear()


if __name__ == "__main__":
    servers = ParseClusterConfiguration("cluster_5.conf")
    for server in servers:
        print("id = {}, ip = {}, log = {}, sm = {}".format(server.id, server.ip, server.log, server.sm))
