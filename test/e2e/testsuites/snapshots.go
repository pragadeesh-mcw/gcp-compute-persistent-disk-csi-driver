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

package testsuites

import (
	"context"
	"fmt"

	"github.com/onsi/ginkgo/v2"
	"k8s.io/kubernetes/test/e2e/framework"
	e2evolume "k8s.io/kubernetes/test/e2e/framework/volume"
	storageframework "k8s.io/kubernetes/test/e2e/storage/framework"
	admissionapi "k8s.io/pod-security-admission/api"
	"sigs.k8s.io/gcp-compute-persistent-disk-csi-driver/test/e2e/driver"
)

type pdCSISnapshotsTestSuite struct {
	tsInfo storageframework.TestSuiteInfo
}

func InitPDCSISnapshotsTestSuite() storageframework.TestSuite {
	return &pdCSISnapshotsTestSuite{
		tsInfo: storageframework.TestSuiteInfo{
			Name: "snapshots",
			TestPatterns: []storageframework.TestPattern{
				storageframework.DefaultFsDynamicPV,
			},
		},
	}
}

func (t *pdCSISnapshotsTestSuite) GetTestSuiteInfo() storageframework.TestSuiteInfo {
	return t.tsInfo
}

func (t *pdCSISnapshotsTestSuite) SkipUnsupportedTests(driver storageframework.TestDriver, pattern storageframework.TestPattern) {
}

func (t *pdCSISnapshotsTestSuite) DefineTests(d storageframework.TestDriver, pattern storageframework.TestPattern) {
	type local struct {
		config         *storageframework.PerTestConfig
		volumeResource *storageframework.VolumeResource
	}
	var l local
	ctx := context.Background()

	f := framework.NewFrameworkWithCustomTimeouts("snapshots", storageframework.GetDriverTimeouts(d))
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged

	init := func() {
		l = local{}
		l.config = d.PrepareTest(ctx, f)
		l.volumeResource = storageframework.CreateVolumeResource(ctx, d, l.config, pattern, e2evolume.SizeRange{})
	}

	cleanup := func() {
		if l.volumeResource != nil {
			err := l.volumeResource.CleanupResource(ctx)
			framework.ExpectNoError(err)
		}
	}

	ginkgo.It("should snapshot and restore a volume", func() {
		init()
		defer cleanup()

		mountPath := "/mnt/test"
		ginkgo.By("Configuring the source pod")
		tPod1 := driver.NewTestPod(f.ClientSet, f.Namespace)
		tPod1.SetupVolume(l.volumeResource, "test-pd-volume", mountPath, false)

		ginkgo.By("Deploying the source pod")
		tPod1.Create(ctx)
		tPod1.WaitForRunning(ctx)

		ginkgo.By("Writing data to the volume")
		tPod1.VerifyExecInPodSucceed(f, fmt.Sprintf("echo 'vital data' > %v/data.txt && sync", mountPath))

		ginkgo.By("Deleting the source pod to ensure volume is detached")
		tPod1.Cleanup(ctx)

		ginkgo.By("Creating a snapshot from the volume")
		snapshotResource := storageframework.CreateSnapshotResource(ctx, d, l.config, l.volumeResource)
		defer snapshotResource.CleanupResource(ctx)

		ginkgo.By("Restoring a new volume from the snapshot")
		restoredVolumeResource := storageframework.CreateVolumeResourceFromSnapshot(ctx, d, l.config, snapshotResource, e2evolume.SizeRange{})
		defer restoredVolumeResource.CleanupResource(ctx)

		ginkgo.By("Configuring the restored pod")
		tPod2 := driver.NewTestPod(f.ClientSet, f.Namespace)
		tPod2.SetupVolume(restoredVolumeResource, "restored-pd-volume", mountPath, false)

		ginkgo.By("Deploying the restored pod")
		tPod2.Create(ctx)
		tPod2.WaitForRunning(ctx)

		ginkgo.By("Verifying data integrity on the restored volume")
		tPod2.VerifyExecInPodSucceed(f, fmt.Sprintf("grep 'vital data' %v/data.txt", mountPath))
	})
}
