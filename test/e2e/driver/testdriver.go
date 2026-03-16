/*
Copyright 2026 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package driver

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	storageframework "k8s.io/kubernetes/test/e2e/storage/framework"
	"sigs.k8s.io/gcp-compute-persistent-disk-csi-driver/pkg/common"
)

type PDCSITestDriver struct {
	driverInfo storageframework.DriverInfo
}

var _ storageframework.TestDriver = &PDCSITestDriver{}
var _ storageframework.DynamicPVTestDriver = &PDCSITestDriver{}

func InitPDCSITestDriver() storageframework.TestDriver {
	return &PDCSITestDriver{
		driverInfo: storageframework.DriverInfo{
			Name: "pd.csi.storage.gke.io",
			SupportedFsType: sets.NewString(
				"ext4",
				"xfs",
			),
			Capabilities: map[storageframework.Capability]bool{
				storageframework.CapPersistence: true,
				storageframework.CapExec:        true,
				storageframework.CapSnapshot:    true,
				storageframework.CapExpansion:   true,
			},
		},
	}
}

func (d *PDCSITestDriver) GetDriverInfo() *storageframework.DriverInfo {
	return &d.driverInfo
}

func (d *PDCSITestDriver) SkipUnsupportedTest(pattern storageframework.TestPattern) {
}

func (d *PDCSITestDriver) PrepareTest(ctx context.Context, f *framework.Framework) *storageframework.PerTestConfig {
	return &storageframework.PerTestConfig{
		Driver:    d,
		Framework: f,
	}
}

func (d *PDCSITestDriver) GetPersistentVolumeSource(readOnly bool, fsType string, volume storageframework.TestVolume) (*corev1.PersistentVolumeSource, *corev1.VolumeNodeAffinity) {
	// This would be implemented for pre-provisioned tests
	return nil, nil
}

func (d *PDCSITestDriver) GetDynamicProvisionStorageClass(ctx context.Context, config *storageframework.PerTestConfig, fsType string) *storagev1.StorageClass {
	params := make(map[string]string)
	if fsType != "" {
		params["csi.storage.k8s.io/fstype"] = fsType
	}
	params["type"] = "pd-balanced"

	return &storagev1.StorageClass{
		TypeMeta: metav1.TypeMeta{
			Kind: "StorageClass",
		},
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "pd-csi-",
		},
		Provisioner: d.driverInfo.Name,
		Parameters:  params,
	}
}
