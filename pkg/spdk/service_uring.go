package spdk

import (
	"fmt"

	"k8s.io/klog/v2"
	"lite.io/liteio/pkg/spdk/jsonrpc/client"
	"lite.io/liteio/pkg/util/misc"
)

type UringBdevCreateRequest struct {
	BdevName  string
	DevPath   string
	BlockSize int
}

type UringBdevDeleteRequest struct {
	BdevName string
}

type UringBdevResizeRequest struct {
	BdevName   string
	TargetSize uint64
}

type UringServiceIface interface {
	CreateUringBdev(req UringBdevCreateRequest) (err error)
	DeleteUringBdev(req UringBdevDeleteRequest) (err error)
	ResizeUringBdev(req UringBdevResizeRequest) (err error)
}

func (svc *SpdkService) CreateUringBdev(req UringBdevCreateRequest) (err error) {
	svc.cli, err = svc.client()
	if err != nil {
		klog.Error("spdk client is nil, try to reconnect spdk socket", err)
		return
	}

	klog.Infof("creating uring_bdev. req is %+v", req)
	var (
		bdevName  = req.BdevName
		devPath   = req.DevPath
		blockSize = req.BlockSize
		hasDev    bool
	)
	// verify devPath exists
	hasDev, err = misc.FileExists(devPath)
	if err != nil || !hasDev {
		err = fmt.Errorf("devPath %s not exists, %t, %+v", devPath, hasDev, err)
		return
	}

	list, err := svc.cli.BdevGetBdevs(client.BdevGetBdevsReq{BdevName: bdevName})
	if err != nil {
		// only return error when error msg is "No such device"
		if !IsNotFoundDeviceError(err) {
			klog.Error(err)
			return
		}
	}
	klog.Infof("devPath %s bdevName %s", devPath, bdevName)

	for _, item := range list {
		if item.Name == bdevName {
			klog.Infof("devpath %s bdev %s already exists", devPath, bdevName)
			return
		}
	}

	_, err = svc.cli.BdevUringCreate(client.BdevUringCreateReq{
		BdevName:  bdevName,
		FileName:  devPath,
		BlockSize: blockSize,
	})
	if err != nil {
		klog.Error(err)
		return
	}

	klog.Infof("created bdev %s", req.BdevName)
	return nil
}

func (svc *SpdkService) DeleteUringBdev(req UringBdevDeleteRequest) (err error) {
	svc.cli, err = svc.client()
	if err != nil {
		klog.Error("spdk client is nil, try to reconnect spdk socket", err)
		return
	}
	// delete uring bdev
	var bdevName = req.BdevName
	var foundBdev bool
	list, err := svc.cli.BdevGetBdevs(client.BdevGetBdevsReq{BdevName: bdevName})
	if err != nil {
		if !IsNotFoundDeviceError(err) {
			return
		}
	}

	if len(list) == 0 {
		klog.Infof("uring_bdev %+s is already deleted", bdevName)
		return nil
	}

	for _, item := range list {
		if item.Name == bdevName {
			foundBdev = true
			klog.Infof("found bdev %s to delete", bdevName)
			break
		}
	}

	if foundBdev {
		result, errRpc := svc.cli.BdevUringDelete(client.BdevUringDeleteReq{Name: bdevName})
		if errRpc != nil || !result {
			err = fmt.Errorf("delete UringBdev %s failed: %t, %+v", bdevName, result, errRpc)
			klog.Error(err)
			return
		}
	} else {
		klog.Infof("not found bdev %s, so consider it deleted", bdevName)
	}

	return
}

func (svc *SpdkService) ResizeUringBdev(req UringBdevResizeRequest) (err error) {
	svc.cli, err = svc.client()
	if err != nil {
		klog.Error("spdk client is nil, try to reconnect spdk socket", err)
		return
	}

	// check uring bdev
	_, err = svc.cli.BdevGetBdevs(client.BdevGetBdevsReq{BdevName: req.BdevName})
	if err != nil {
		klog.Error(err)
		return
	}

	var result bool
	result, err = svc.cli.BdevUringResize(client.BdevUringResizeReq{
		Name: req.BdevName,
		Size: req.TargetSize,
	})
	if err != nil || !result {
		err = fmt.Errorf("resize UringBdev %s failed: %t, %+v", req.BdevName, result, err)
		klog.Error(err)
		return
	}

	return
}
