package osutil

import (
	"encoding/json"
	"errors"
)

type NVMeUtilityIface interface {
	ListNVMePaths() (paths []string, err error)
}

type Devices struct {
	Devices []Device `json:"Devices"`
}

type Device struct {
	NameSpace    uint64 `json:"NameSpace"`
	DevicePath   string `json:"DevicePath"`
	Firmware     string `json:"Firmware"`
	Index        uint64 `json:"Index"`
	ModelNumber  string `json:"ModelNumber"`
	SerialNumber string `json:"SerialNumber"`
	UsedBytes    uint64 `json:"UsedBytes"`
	MaximumLBA   uint64 `json:"MaximumLBA"`
	PhysicalSize uint64 `json:"PhysicalSize"`
	SectorSize   uint64 `json:"SectorSize"`
}

type NVMeUtil struct {
	exec ShellExec
}

func NewNVMeUtil(exec ShellExec) *NVMeUtil {
	return &NVMeUtil{
		exec: exec,
	}
}

func (nu *NVMeUtil) ListNVMePaths() (paths []string, err error) {
	shell := `set -o pipefail; nvme list -o json`

	out, err := nu.exec.ExecCmd("bash", []string{"-c", shell})
	if err != nil {
		err = errors.New(err.Error() + string(out))
		return
	}

	var devices Devices
	err = json.Unmarshal(out, &devices)
	if err != nil {
		err = errors.New(err.Error() + string(out))
		return
	}

	for _, device := range devices.Devices {
		paths = append(paths, device.DevicePath)
	}

	return
}
