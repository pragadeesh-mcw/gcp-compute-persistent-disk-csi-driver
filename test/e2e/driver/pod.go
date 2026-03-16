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
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	storageframework "k8s.io/kubernetes/test/e2e/storage/framework"
	imageutils "k8s.io/kubernetes/test/utils/image"
)

type TestPod struct {
	client    clientset.Interface
	pod       *corev1.Pod
	namespace *corev1.Namespace
}

func NewTestPod(c clientset.Interface, ns *corev1.Namespace) *TestPod {
	return &TestPod{
		client:    c,
		namespace: ns,
		pod: &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: "pd-csi-tester-",
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{
						Name:    "volume-tester",
						Image:   imageutils.GetE2EImage(imageutils.BusyBox),
						Command: []string{"/bin/sh"},
						Args:    []string{"-c", "tail -f /dev/null"},
					},
				},
				RestartPolicy: corev1.RestartPolicyAlways,
			},
		},
	}
}

func (t *TestPod) SetupVolume(volumeResource *storageframework.VolumeResource, name, mountPath string, readOnly bool) {
	volume := corev1.Volume{
		Name: name,
	}
	if volumeResource.Pvc != nil {
		volume.VolumeSource = corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
				ClaimName: volumeResource.Pvc.Name,
			},
		}
	} else if volumeResource.VolSource != nil {
		volume.VolumeSource = *volumeResource.VolSource
	}

	t.pod.Spec.Volumes = append(t.pod.Spec.Volumes, volume)
	t.pod.Spec.Containers[0].VolumeMounts = append(t.pod.Spec.Containers[0].VolumeMounts, corev1.VolumeMount{
		Name:      name,
		MountPath: mountPath,
		ReadOnly:  readOnly,
	})
}

func (t *TestPod) Create(ctx context.Context) {
	var err error
	t.pod, err = t.client.CoreV1().Pods(t.namespace.Name).Create(ctx, t.pod, metav1.CreateOptions{})
	framework.ExpectNoError(err)
}

func (t *TestPod) WaitForRunning(ctx context.Context) {
	err := e2epod.WaitTimeoutForPodRunningInNamespace(ctx, t.client, t.pod.Name, t.pod.Namespace, framework.PodStartTimeout)
	framework.ExpectNoError(err)
}

func (t *TestPod) VerifyExecInPodSucceed(f *framework.Framework, shExec string) {
	_, _, err := e2epod.ExecCommandInContainerWithFullOutput(f, t.pod.Name, t.pod.Spec.Containers[0].Name, "/bin/sh", "-c", shExec)
	framework.ExpectNoError(err)
}

func (t *TestPod) Cleanup(ctx context.Context) {
	e2epod.DeletePodOrFail(ctx, t.client, t.namespace.Name, t.pod.Name)
}
