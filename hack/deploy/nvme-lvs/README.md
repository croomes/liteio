# Install

```console
echo 1024 | tee /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages
systemctl restart kubelet
modprobe nvme-tcp
modprobe vfio-pci
tdnf install -y nvme-cli pciutils fuse3-devel liburing-devel
```

## SPDK build

```console
cd /var/tmp
git clone https://github.com/spdk/spdk
cd spdk
git submodule update --init
./scripts/pkgdep.sh --uring
./configure --without-nvme-cuse --with-uring
make
```
