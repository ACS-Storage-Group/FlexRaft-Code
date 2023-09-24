import subprocess
import os

r = os.popen("pwd")
root_path = r.read()[:-1] + "/dep"
r.close()
print("pwd={}".format(root_path))
if os.path.exists(root_path):
    subprocess.run("sudo rm -rf {}".format(root_path), shell=True)
os.mkdir(root_path)
os.chdir(root_path)


def install_rocksdb():
    if not os.path.exists("rocksdb"):
        subprocess.run("git clone https://github.com/facebook/rocksdb.git", shell=True)
    os.chdir("rocksdb")
    subprocess.run("git checkout v7.4.5", shell=True)
    subprocess.run("make static_lib -j 8", shell=True)
    subprocess.run("sudo make install", shell=True)
    os.chdir(root_path)

def install_gtest():
    if not os.path.exists("googletest"):
        subprocess.run("git clone https://github.com/google/googletest.git", shell=True)
    os.chdir("googletest")
    subprocess.run("cmake -B build", shell=True)
    subprocess.run("cmake --build build", shell=True)
    os.chdir("build")
    subprocess.run("sudo make install", shell=True)
    os.chdir(root_path)

def install_rocksdb_dependencies_centos():
    print("Install RocksDB Dependencies for CentOS")
    subprocess.run("sudo yum makecache", shell=True)
    subprocess.run("sudo yum -y install uuid uuid-devel libuuid libuuid-devel", shell=True)
    subprocess.run("sudo yum -y install zlib-devel", shell=True)
    subprocess.run("sudo yum -y install bzip2-devel", shell=True)
    subprocess.run("sudo yum -y install lz4-devel", shell=True)
    subprocess.run("sudo yum -y install snappy-devel", shell=True)
    subprocess.run("sudo yum -y install libzstd-devel", shell=True)
    subprocess.run("sudo yum -y install gflags-devel", shell=True)

def install_rocksdb_dependencies_ubuntu():
    print("Install RocksDB Dependencies for Ubuntu")
    subprocess.run("sudo apt-get install -y uuid-dev", shell=True)
    subprocess.run("sudo apt-get install -y zlib1g-dev", shell=True)
    subprocess.run("sudo apt-get install -y libbz2-dev", shell=True)
    subprocess.run("sudo apt-get install -y liblz4-dev", shell=True)
    subprocess.run("sudo apt-get install -y libsnappy-dev", shell=True)
    subprocess.run("sudo apt-get install -y libzstd-dev", shell=True)
    subprocess.run("sudo apt-get install -y libgflags-dev", shell=True)

def install_rocksdb_dependencies():
    result = os.popen("lsb_release -a")
    res = result.read()
    if res.find("Ubuntu") != -1:
        install_rocksdb_dependencies_ubuntu()
    elif res.find("CentOS") != -1:
        install_rocksdb_dependencies_centos()
    else:
        print("Unsupported Platform")


def install_nasm_assembler():
    subprocess.run("rm -rf nasm-2.14.02.tar.gz nasm-2.14.02", shell=True)
    subprocess.run("wget https://www.nasm.us/pub/nasm/releasebuilds/2.14.02/nasm-2.14.02.tar.gz", shell=True)
    subprocess.run("tar -zxvf nasm-2.14.02.tar.gz", shell=True)
    os.chdir("nasm-2.14.02")
    subprocess.run("./autogen.sh", shell=True)
    subprocess.run("./configure", shell=True)
    subprocess.run("sudo make install", shell=True)
    os.chdir(root_path)

def install_libisal():
    subprocess.run("rm -rf isa-l", shell=True)
    subprocess.run("git clone https://github.com/intel/isa-l.git -b v2.30.0", shell=True)
    os.chdir("isa-l")
    subprocess.run("./autogen.sh", shell=True)
    subprocess.run("./configure", shell=True)
    subprocess.run("sudo make install", shell=True)
    os.chdir(root_path)

if __name__ == "__main__":
    install_gtest()
    install_rocksdb_dependencies()
    install_rocksdb()
    install_nasm_assembler()
    install_libisal()
    subprocess.run("sudo rm -rf {}".format(root_path), shell=True)


