import subprocess
import os

def install_rocksdb():
    if not os.path.exists("v7.4.5.tar.gz"):
        subprocess.run("wget https://github.com/facebook/rocksdb/archive/refs/tags/v7.4.5.tar.gz", shell=True)
    if not os.path.exists("rocksdb-7.4.5"):
        subprocess.run("tar -zxvf v7.4.5.tar.gz", shell=True)
    os.chdir("rocksdb-7.4.5")
    subprocess.run("make static_lib -j 8", shell=True)
    subprocess.run("sudo make install", shell=True)
    os.chdir("..")
    subprocess.run("rm -rf rocksdb-7.4.5 && rm -rf v7.4.5.tar.gz", shell=True)

def install_gtest():
    subprocess.run("git clone git@github.com:google/googletest.git", shell=True)
    os.chdir("googletest")
    subprocess.run("cmake3 -B build", shell=True)
    subprocess.run("cmake3 --build build", shell=True)
    os.chdir("build")
    subprocess.run("make install", shell=True)

def install_rocksdb_dependencies_centos():
    print("Install RocksDB Dependencies for CentOS")
    subprocess.run("sudo yum makecache", shell=True)
    subprocess.run("sudo yum -y install uuid uuid-devel libuuid libuuid-devel", shell=True)
    subprocess.run("sudo yum -y install zlib-devel", shell=True)
    subprocess.run("sudo yum -y install bzip2-devel", shell=True)
    subprocess.run("sudo yum -y install lz4-devel", shell=True)
    subprocess.run("sudo yum -y install snappy-devel", shell=True)
    subprocess.run("sudo yum -y install libzstd-devel", shell=True)

def install_rocksdb_dependencies_ubuntu():
    print("Install RocksDB Dependencies for Ubuntu")
    subprocess.run("apt-get install -y uuid-devel", shell=True)
    subprocess.run("apt-get install -y zlib1g-dev", shell=True)
    subprocess.run("apt-get install -y libbz2-dev", shell=True)
    subprocess.run("apt-get install -y liblz4-dev", shell=True)
    subprocess.run("apt-get install -y libsnappy-dev", shell=True)
    subprocess.run("apt-get install -y libzstd-dev", shell=True)

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
    subprocess.run("cd nams-2.14.02", shell=True)
    subprocess.run("./autogen.sh", shell=True)
    subprocess.run("./configure", shell=True)
    subprocess.run("make install", shell=True)

def install_libisal():
    subprocess.run("rm -rf isa-l", shell=True)
    subprocess.run("git clone git@github.com:intel/isa-l.git", shell=True)
    subprocess.run("cd isa-l", shell=True)
    subprocess.run("./autogen.sh", shell=True)
    subprocess.run("./configure", shell=True)
    subprocess.run("make install", shell=True)

if __name__ == "__main__":
    install_gtest()
    install_rocksdb_dependencies()
    install_rocksdb()
    install_nasm_assembler()
    install_libisal()


