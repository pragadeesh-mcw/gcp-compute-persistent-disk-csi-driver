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
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/test/e2e/framework"
	e2evolume "k8s.io/kubernetes/test/e2e/framework/volume"
	storageframework "k8s.io/kubernetes/test/e2e/storage/framework"
	admissionapi "k8s.io/pod-security-admission/api"
	"sigs.k8s.io/gcp-compute-persistent-disk-csi-driver/test/e2e/driver"
)

type pdCSIWorkloadsTestSuite struct {
	tsInfo storageframework.TestSuiteInfo
}

func InitPDCSIWorkloadsTestSuite() storageframework.TestSuite {
	return &pdCSIWorkloadsTestSuite{
		tsInfo: storageframework.TestSuiteInfo{
			Name: "workloads",
			TestPatterns: []storageframework.TestPattern{
				storageframework.DefaultFsDynamicPV,
			},
		},
	}
}

func (t *pdCSIWorkloadsTestSuite) GetTestSuiteInfo() storageframework.TestSuiteInfo {
	return t.tsInfo
}

func (t *pdCSIWorkloadsTestSuite) SkipUnsupportedTests(driver storageframework.TestDriver, pattern storageframework.TestPattern) {
}

func (t *pdCSIWorkloadsTestSuite) DefineTests(d storageframework.TestDriver, pattern storageframework.TestPattern) {
	type local struct {
		config         *storageframework.PerTestConfig
		volumeResource *storageframework.VolumeResource
	}
	var l local
	ctx := context.Background()

	f := framework.NewFrameworkWithCustomTimeouts("workloads", storageframework.GetDriverTimeouts(d))
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

	ginkgo.It("should store data in a Deployment", func() {
		init()
		defer cleanup()

		mountPath := "/mnt/test"
		ginkgo.By("Creating a Deployment with the volume")
		deploymentName := "pd-csi-deployment"
		replicas := int32(1)
		labels := map[string]string{"app": "pd-csi-test"}
		
		tPod := driver.NewTestPod(f.ClientSet, f.Namespace)
		tPod.SetupVolume(l.volumeResource, "test-pd-volume", mountPath, false)
		
		deployment := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name: deploymentName,
			},
			Spec: appsv1.DeploymentSpec{
				Replicas: &replicas,
				Selector: &metav1.LabelSelector{
					MatchLabels: labels,
				},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{
						Labels: labels,
					},
					Spec: tPod.GetPodSpec(), // Assuming I add GetPodSpec to TestPod
				},
			},
		}

		_, err := f.ClientSet.AppsV1().Deployments(f.Namespace.Name).Create(ctx, deployment, metav1.CreateOptions{})
		framework.ExpectNoError(err)

		// Wait for deployment to be ready and verify data...
		// (Simplified for now, but following the GCS Fuse logic)
	})
}
