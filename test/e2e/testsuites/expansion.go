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
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/test/e2e/framework"
	e2evolume "k8s.io/kubernetes/test/e2e/framework/volume"
	storageframework "k8s.io/kubernetes/test/e2e/storage/framework"
	admissionapi "k8s.io/pod-security-admission/api"
	"sigs.k8s.io/gcp-compute-persistent-disk-csi-driver/test/e2e/driver"
)

type pdCSIExpansionTestSuite struct {
	tsInfo storageframework.TestSuiteInfo
}

func InitPDCSIExpansionTestSuite() storageframework.TestSuite {
	return &pdCSIExpansionTestSuite{
		tsInfo: storageframework.TestSuiteInfo{
			Name: "expansion",
			TestPatterns: []storageframework.TestPattern{
				storageframework.DefaultFsDynamicPV,
			},
		},
	}
}

func (t *pdCSIExpansionTestSuite) GetTestSuiteInfo() storageframework.TestSuiteInfo {
	return t.tsInfo
}

func (t *pdCSIExpansionTestSuite) SkipUnsupportedTests(driver storageframework.TestDriver, pattern storageframework.TestPattern) {
}

func (t *pdCSIExpansionTestSuite) DefineTests(d storageframework.TestDriver, pattern storageframework.TestPattern) {
	type local struct {
		config         *storageframework.PerTestConfig
		volumeResource *storageframework.VolumeResource
	}
	var l local
	ctx := context.Background()

	f := framework.NewFrameworkWithCustomTimeouts("expansion", storageframework.GetDriverTimeouts(d))
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged

	init := func() {
		l = local{}
		l.config = d.PrepareTest(ctx, f)
		l.volumeResource = storageframework.CreateVolumeResource(ctx, d, l.config, pattern, e2evolume.SizeRange{Min: "10Gi", Max: "10Gi"})
	}

	cleanup := func() {
		if l.volumeResource != nil {
			err := l.volumeResource.CleanupResource(ctx)
			framework.ExpectNoError(err)
		}
	}

	ginkgo.It("should expand a volume online", func() {
		init()
		defer cleanup()

		mountPath := "/mnt/test"
		ginkgo.By("Configuring the pod")
		tPod := driver.NewTestPod(f.ClientSet, f.Namespace)
		tPod.SetupVolume(l.volumeResource, "test-pd-volume", mountPath, false)

		ginkgo.By("Deploying the pod")
		tPod.Create(ctx)
		tPod.WaitForRunning(ctx)

		ginkgo.By("Verifying initial filesystem size")
		tPod.VerifyExecInPodSucceed(f, fmt.Sprintf("df -h %v | grep 10G", mountPath))

		ginkgo.By("Expanding the PVC")
		newSize := resource.MustParse("20Gi")
		l.volumeResource.Pvc.Spec.Resources.Requests[corev1.ResourceStorage] = newSize
		var err error
		l.volumeResource.Pvc, err = f.ClientSet.CoreV1().PersistentVolumeClaims(f.Namespace.Name).Update(ctx, l.volumeResource.Pvc, metav1.UpdateOptions{})
		framework.ExpectNoError(err)

		ginkgo.By("Waiting for filesystem resize in the pod")
		// The storageframework should ideally handle waiting for expansion, but we can add a manual wait if needed.
		// For now, we'll use a simple Eventually-like check.
		tPod.VerifyExecInPodSucceed(f, fmt.Sprintf("while ! (df -h %v | grep 20G); do sleep 5; done", mountPath))
	})
}
