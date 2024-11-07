package pool

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"k8s.io/klog/v2"
	"lite.io/liteio/pkg/agent/config"
	"lite.io/liteio/pkg/agent/pool/engine"
	v1 "lite.io/liteio/pkg/api/volume.antstor.alipay.com/v1"
	"lite.io/liteio/pkg/spdk"
	"lite.io/liteio/pkg/spdk/jsonrpc/client"
	"lite.io/liteio/pkg/util/lvm"
	"lite.io/liteio/pkg/util/misc"
	"lite.io/liteio/pkg/util/osutil"
)

const (
	defaultBlockSize = 512
)

var (
	_ PoolBuilderIface = &PoolBuilder{}
)

type PoolBuilderIface interface {
	WithConfig(cfg config.StorageStack) (bld PoolBuilderIface)
	WithMode(mode v1.PoolMode) (bld PoolBuilderIface)
	WithPoolEngine(eng engine.PoolEngineIface) (bld PoolBuilderIface)
	WithSpdkService(spdk spdk.SpdkServiceIface) (bld PoolBuilderIface)
	Build() (info engine.StaticInfo, err error)
}

type PoolBuilder struct {
	mode v1.PoolMode
	cfg  config.StorageStack

	eng  engine.PoolEngineIface
	spdk spdk.SpdkServiceIface
	kmod osutil.KmodUtilityIface
	pci  osutil.PCIUtilityIface
	nvme osutil.NVMeUtilityIface
}

func NewPoolBuilder() *PoolBuilder {
	return &PoolBuilder{
		kmod: osutil.NewKmodUtil(osutil.NewCommandExec()),
		pci:  osutil.NewPCIUtil(osutil.NewCommandExec()),
		nvme: osutil.NewNVMeUtil(osutil.NewCommandExec()),
	}
}

func (pb *PoolBuilder) WithConfig(cfg config.StorageStack) (bld PoolBuilderIface) {
	pb.cfg = cfg
	return pb
}

func (pb *PoolBuilder) WithMode(mode v1.PoolMode) (bld PoolBuilderIface) {
	pb.mode = mode
	return pb
}

func (pb *PoolBuilder) WithSpdkService(spdk spdk.SpdkServiceIface) (bld PoolBuilderIface) {
	pb.spdk = spdk
	return pb
}

func (pb *PoolBuilder) WithPoolEngine(eng engine.PoolEngineIface) (bld PoolBuilderIface) {
	pb.eng = eng
	return pb
}

func (pb *PoolBuilder) Build() (info engine.StaticInfo, err error) {
	switch pb.mode {
	case v1.PoolModeKernelLVM:
		info, err = pb.buildLVM(pb.cfg)
	case v1.PoolModeSpdkLVStore:
		info, err = pb.buildSpdkLVS(pb.cfg)
	default:
		err = fmt.Errorf("invalid agent Mode %s", pb.mode)
	}

	return
}

func (pb *PoolBuilder) buildLVM(cfg config.StorageStack) (info engine.StaticInfo, err error) {
	// Get vg by name cfg.Pooling.Name
	info, err = pb.eng.PoolInfo(cfg.Pooling.Name)
	if err == nil {
		if info.LVM != nil {
			klog.Infof("successfully read vg info: %+v", *info.LVM)
		}
		return
	}

	if errors.Is(err, engine.ErrNotFoundVG) {
		klog.Info("creating LVM VG ", pb.cfg)
		var pvs []string

		for idx, pv := range pb.cfg.PVs {
			// set default device path
			if pv.DevicePath == "" {
				pv.DevicePath = fmt.Sprintf("/dev/loop%d", idx)
			}
			// check if device exists
			if has, _ := misc.FileExists(pv.DevicePath); has {
				klog.Infof("pv device %s exists", pv.DevicePath)
				pvs = append(pvs, pv.DevicePath)
				continue
			}

			// vlaidate pv to create
			if pv.FilePath == "" || pv.Size == 0 {
				err = fmt.Errorf("invalid config of PV, %+v", pv)
				klog.Error(err)
				return
			}
			// if device not exist, create this dev
			// create file with size, create loop device
			err = misc.CreateFallocateFile(pv.FilePath, int64(pv.Size))
			if err != nil {
				klog.Error(err)
				return
			}

			// create PV
			loopDev := fmt.Sprintf("/dev/loop%d", idx)
			klog.Infof("forcely set DevicePath %s to %s ", pv.DevicePath, loopDev)
			pv.DevicePath = loopDev
			err = osutil.CreateLoopDevice(osutil.NewCommandExec(), pv.DevicePath, pv.FilePath)
			if err != nil {
				klog.Error(err)
				return
			}
			pvs = append(pvs, pv.DevicePath)
		}

		// create PVs and VG
		klog.Infof("create vg %s by pvs %+v", cfg.Pooling.Name, pvs)
		if len(pvs) == 0 {
			err = fmt.Errorf("no PVs to create vg")
			return
		}
		err = lvm.LvmUtil.CreatePV(pvs)
		if err != nil {
			klog.Error(err)
			return
		}
		var vg lvm.VG
		vg, err = lvm.LvmUtil.CreateVG(cfg.Pooling.Name, pvs)
		if err != nil {
			klog.Error(err)
			return
		}
		klog.Infof("created vg %+v", vg)

		info, err = pb.eng.PoolInfo(cfg.Pooling.Name)
		if err == nil {
			klog.Infof("successfully read vg info: %+v", info)
			return
		}

		return
	}

	return
}

func (pb *PoolBuilder) buildSpdkLVS(cfg config.StorageStack) (info engine.StaticInfo, err error) {
	// check lvs existance
	var lvs client.LVStoreInfo
	lvs, err = pb.spdk.GetLVStore(cfg.Bdev.Name)
	if err == nil {
		klog.Infof("lvs already exists, %+v", lvs)
		// assemble pool
		info.LVS = &v1.SpdkLVStore{
			Name:             lvs.Name,
			UUID:             lvs.UUID,
			BaseBdev:         lvs.BaseBdev,
			ClusterSize:      lvs.ClusterSize,
			TotalDataCluster: lvs.TotalDataClusters,
			BlockSize:        lvs.BlockSize,
			Bytes:            uint64(lvs.ClusterSize * lvs.TotalDataClusters),
		}
		return
	}

	// ensure base bdev exist
	if spdk.IsNotFoundDeviceError(err) {
		switch cfg.Bdev.Type {
		case config.AioBdevType:
			err = pb.buildLvsAioBdev(cfg)
		case config.UringBdevType:
			err = pb.buildLvsUringBdev(cfg)
		case config.MemBdevType:
			err = pb.buildLvsMemBdev(cfg)
		case config.RaidBdevType:
			// TODO: refactor
			return pb.buildLvsRaidBdev(cfg)
		default:
			err = fmt.Errorf("not supported bdev type %s", cfg.Bdev.Type)
		}

		if err != nil {
			klog.Error(err)
			return
		}
	}

	// create lvs
	lvs, err = pb.spdk.CreateLVStore(spdk.CreateLVStoreReq{
		BdevName:    cfg.Bdev.Name,
		LVStoreName: cfg.Pooling.Name,
	})
	if err != nil {
		klog.Error(err)
		return
	}
	// assemble pool
	info.LVS = &v1.SpdkLVStore{
		Name:             lvs.Name,
		UUID:             lvs.UUID,
		BaseBdev:         lvs.BaseBdev,
		ClusterSize:      lvs.ClusterSize,
		TotalDataCluster: lvs.TotalDataClusters,
		BlockSize:        lvs.BlockSize,
		Bytes:            uint64(lvs.ClusterSize * lvs.TotalDataClusters),
	}

	return
}

func (pb *PoolBuilder) buildLvsAioBdev(cfg config.StorageStack) (err error) {
	// check if base bdev exists
	var list []spdk.Bdev
	list, err = pb.spdk.BdevGetBdevs(spdk.BdevGetBdevsReq{
		BdevName: cfg.Bdev.Name,
	})
	if err != nil && spdk.IsNotFoundDeviceError(err) {
		// check file existance
		var errFile error
		_, errFile = os.Stat(cfg.Bdev.FilePath)
		if os.IsNotExist(errFile) {
			// create file with size
			var f *os.File
			f, err = os.Create(cfg.Bdev.FilePath)
			if err != nil {
				klog.Error(err)
				return
			}
			err = f.Truncate(int64(cfg.Bdev.Size))
			if err != nil {
				klog.Error(err)
				return
			}
		}

		// create aio_bdev with name and file
		pb.spdk.CreateAioBdev(spdk.AioBdevCreateRequest{
			BdevName:  cfg.Bdev.Name,
			DevPath:   cfg.Bdev.FilePath,
			BlockSize: defaultBlockSize,
		})
	}

	klog.Infof("base bdev %s may exist, first query list %+v", cfg.Bdev.Name, list)

	return
}

func (pb *PoolBuilder) buildLvsUringBdev(cfg config.StorageStack) (err error) {
	klog.Info("building lvstore with io uring bdev")
	// check if base bdev exists
	var list []spdk.Bdev
	list, err = pb.spdk.BdevGetBdevs(spdk.BdevGetBdevsReq{
		BdevName: cfg.Bdev.Name,
	})
	if err != nil && spdk.IsNotFoundDeviceError(err) {
		klog.Info("io uring bdev not found, create it")
		// check file existance
		var info fs.FileInfo
		var errFile error
		info, errFile = os.Stat(cfg.Bdev.FilePath)
		if os.IsNotExist(errFile) {
			klog.Errorf("filepath %s does not exist: %w", cfg.Bdev.FilePath, err)
			return
		}
		if info.Mode()&fs.ModeDevice == 0 {
			klog.Errorf("%s is a not a device", cfg.Bdev.FilePath)
			return
		}

		// create uring_bdev with name and file
		if err = pb.spdk.CreateUringBdev(spdk.UringBdevCreateRequest{
			BdevName:  cfg.Bdev.Name,
			DevPath:   cfg.Bdev.FilePath,
			BlockSize: defaultBlockSize,
		}); err != nil {
			klog.Error(err)
			return
		}
	}

	klog.Infof("base bdev %s may exist, first query list %+v", cfg.Bdev.Name, list)

	return
}

func (pb *PoolBuilder) buildLvsMemBdev(cfg config.StorageStack) (err error) {
	// check if base bdev exists
	var list []spdk.Bdev
	list, err = pb.spdk.BdevGetBdevs(spdk.BdevGetBdevsReq{
		BdevName: cfg.Bdev.Name,
	})
	if err != nil && spdk.IsNotFoundDeviceError(err) {
		// create mem_bdev with name and size
		blocks := cfg.Bdev.Size / defaultBlockSize
		if blocks == 0 {
			err = fmt.Errorf("invalid bdev size: %d, should be bigger than block size 512 byte", cfg.Bdev.Size)
			return
		}

		err = pb.spdk.CreateMemBdev(spdk.CreateBdevMallocReq{
			Name:      cfg.Bdev.Name,
			BlockSize: defaultBlockSize,
			NumBlocks: int(blocks),
		})
		if err != nil {
			klog.Error(err)
			return
		}
	}

	klog.Infof("base bdev %s may exist, first query list %+v", cfg.Bdev.Name, list)

	return
}

func (pb *PoolBuilder) buildLvsRaidBdev(cfg config.StorageStack) (info engine.StaticInfo, err error) {
	var lvs spdk.LVStoreInfo

	switch cfg.Bdev.Type {
	case config.RaidBdevType:
		klog.Info("building lvstore. unbind nvme and bind vfio-pci.")

		var nvmeIDList []string
		nvmeIDList, err = pb.prepareNVMePCI()
		if err != nil {
			klog.Error(err)
			return
		}

		// attach controller, create raid bdev, create lvstore
		// create lvstore from pcie devices
		// TODO: use raid name and lvs name from config

		lvs, err = pb.spdk.CreateLVStoreFromNVMeIDs(spdk.AttachNVMeReq{
			NVMeIDs: nvmeIDList,
		})
		if err != nil {
			klog.Error(err)
			return
		}
	case config.UringBdevType:
		klog.Info("building lvstore with io uring bdevs.")

		var nvmePathList []string
		nvmePathList, err = pb.prepareNVMeIOUring()
		if err != nil {
			klog.Error(err)
			return
		}

		for _, path := range nvmePathList {
			klog.Infof("nvme path %s", path)

			name := filepath.Base(path)

			// check if  bdev exists
			_, err = pb.spdk.BdevGetBdevs(spdk.BdevGetBdevsReq{
				BdevName: name,
			})
			if err != nil && spdk.IsNotFoundDeviceError(err) {
				// check file existance
				info, errFile := os.Stat(cfg.Bdev.FilePath)
				if os.IsNotExist(errFile) {
					klog.Error(err)
					continue
				}
				if info.Mode()&fs.ModeDevice == 0 {
					klog.Error("file is a not a device")
					continue
				}

				// create uring_bdev with name and file
				err = pb.spdk.CreateUringBdev(spdk.UringBdevCreateRequest{
					BdevName:  name,
					DevPath:   path,
					BlockSize: defaultBlockSize,
				})
				if err != nil {
					klog.Error("uring bdev creation failed", err)
					continue
				}
			}

		}

		lvs, err = pb.spdk.CreateLVStoreFromNVMeDevicePaths(spdk.AttachNVMeReq{
			NVMeIDs: nvmePathList,
		})
		if err != nil {
			klog.Error("lvstore creation with io uring bdevs failed", err)
			return
		}
	}

	info.LVS = &v1.SpdkLVStore{
		Name:             lvs.Name,
		UUID:             lvs.UUID,
		BaseBdev:         lvs.BaseBdev,
		ClusterSize:      lvs.ClusterSize,
		TotalDataCluster: lvs.TotalDataClusters,
		BlockSize:        lvs.BlockSize,
		Bytes:            uint64(lvs.ClusterSize * lvs.TotalDataClusters),
	}

	klog.Info("successfully created lvstore", info.LVS)
	return
}

func (pb *PoolBuilder) prepareNVMePCI() ([]string, error) {
	var err error
	if err = pb.kmod.HasKmod("vfio_pci"); err != nil {
		err = pb.kmod.ProbeKmod("vfio-pci")
		if err != nil {
			klog.Error("installing mod vfio-pci failed. ", err)
			return nil, err
		}
	}
	// check kmod of vfio_iommu_type1
	if err = pb.kmod.HasKmod("vfio_iommu_type1"); err != nil {
		err = pb.kmod.ProbeKmod("vfio_iommu_type1")
		if err != nil {
			klog.Error("installing mod vfio_iommu_type1 failed. ", err)
			return nil, err
		}
	}

	// unbind NVMe from nvme driver
	var nvmeIDList []string
	var nvmeTypeID string
	nvmeIDList, err = pb.pci.ListNVMeID()
	if err != nil {
		klog.Error("list nvme devices failed, err ", err)
		return nil, err
	}
	if len(nvmeIDList) > 0 {
		for _, id := range nvmeIDList {
			// if nvme exists in nvme driver, do unbind
			if errExist := pb.pci.CheckNVMeExistence(id, osutil.NVMeDriverName); errExist == nil {
				klog.Infof("unbind PCIe device %s from nvme", id)
				err = pb.pci.UnbindNVMe(id, osutil.NVMeDriverName)
				if err != nil {
					klog.Error(err)
					return nil, err
				}
			} else {
				klog.Infof("PCIe %s is already not controled by nvme", id)
			}
		}
		// bind nvme to vfio-pci driver
		nvmeTypeID, err = pb.pci.GetNVMeTypeID(nvmeIDList[0])
		if err != nil {
			klog.Error(err)
			return nil, err
		}
		klog.Infof("binding NVMe Type %s to vfio-pci", nvmeTypeID)
		err = pb.pci.BindNVMeByType(nvmeTypeID, osutil.VfioPCIDriverName)
		if err != nil {
			klog.Error(err)
			return nil, err
		}

		// TODO: wait PCIe appearing in vfio-pci driver
		time.Sleep(5 * time.Second)
	}

	return nvmeIDList, nil
}

func (pb *PoolBuilder) prepareNVMeIOUring() ([]string, error) {
	nvmePathList, err := pb.nvme.ListNVMePaths()
	if err != nil {
		klog.Error("list nvme devices failed, err ", err)
		return nil, err
	}

	return nvmePathList, nil
}
