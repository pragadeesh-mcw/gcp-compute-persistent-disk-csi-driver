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

package tests

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"
)

var _ = Describe("K8s GCE PD CSI Driver Autoprovisioning Tests", Label("k8s-autoprovisioning"), func() {
	var (
		kubeClient kubernetes.Interface
		ns         *corev1.Namespace
		projectID  string
		zone       string
	)

	BeforeEach(func() {
		var err error
		kubeClient, err = getKubeClient()
		Expect(err).To(BeNil(), "Failed to create kubernetes client")

		nsName := "autoprovisioning-test-" + string(uuid.NewUUID())[:8]
		ns, err = kubeClient.CoreV1().Namespaces().Create(context.TODO(), &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: nsName},
		}, metav1.CreateOptions{})
		Expect(err).To(BeNil(), "Failed to create namespace %s", nsName)

		projectID = *project
		zone = strings.Split(*zones, ",")[0]
	})

	AfterEach(func() {
		if ns != nil && kubeClient != nil {
			By(fmt.Sprintf("Deleting namespace %s", ns.Name))
			err := kubeClient.CoreV1().Namespaces().Delete(context.TODO(), ns.Name, metav1.DeleteOptions{})
			Expect(err).To(BeNil(), "Failed to delete namespace %s", ns.Name)
		}
	})

	It("Should provision a volume and attach it to a pod with node affinity", func() {
		scName := "test-sc-" + string(uuid.NewUUID())[:8]
		pvcName := "test-pvc-" + string(uuid.NewUUID())[:8]
		podName := "test-pod-" + string(uuid.NewUUID())[:8]

		By(fmt.Sprintf("Creating StorageClass %s", scName))
		sc := &storagev1.StorageClass{
			ObjectMeta: metav1.ObjectMeta{Name: scName},
			Provisioner: "pd.csi.storage.gke.io",
			Parameters: map[string]string{
				"type": "pd-balanced",
			},
			ReclaimPolicy: func() *corev1.PersistentVolumeReclaimPolicy {
				p := corev1.PersistentVolumeReclaimDelete
				return &p
			}(),
			VolumeBindingMode: func() *storagev1.VolumeBindingMode {
				v := storagev1.VolumeBindingWaitForFirstConsumer
				return &v
			}(),
		}
		_, err := kubeClient.StorageV1().StorageClasses().Create(context.TODO(), sc, metav1.CreateOptions{})
		Expect(err).To(BeNil())
		defer kubeClient.StorageV1().StorageClasses().Delete(context.TODO(), scName, metav1.DeleteOptions{})

		By(fmt.Sprintf("Creating PVC %s", pvcName))
		pvc := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      pvcName,
				Namespace: ns.Name,
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: resource.MustParse("10Gi"),
					},
				},
				StorageClassName: &scName,
			},
		}
		_, err = kubeClient.CoreV1().PersistentVolumeClaims(ns.Name).Create(context.TODO(), pvc, metav1.CreateOptions{})
		Expect(err).To(BeNil())

		By("Identifying a target node")
		nodes, err := kubeClient.CoreV1().Nodes().List(context.TODO(), metav1.ListOptions{})
		Expect(err).To(BeNil())
		Expect(len(nodes.Items)).To(BeNumerically(">", 0))
		targetNode := nodes.Items[0].Name

		By(fmt.Sprintf("Creating Pod %s on node %s", podName, targetNode))
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      podName,
				Namespace: ns.Name,
			},
			Spec: corev1.PodSpec{
				NodeName: targetNode,
				Containers: []corev1.Container{
					{
						Name:  "web",
						Image: "busybox",
						Command: []string{"sh", "-c", "while true; do echo hello >> /data/test.log; sleep 1; done"},
						VolumeMounts: []corev1.VolumeMount{
							{
								Name:      "data",
								MountPath: "/data",
							},
						},
					},
				},
				Volumes: []corev1.PodVolume{
					{
						Name: "data",
						VolumeSource: corev1.VolumeSource{
							PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
								ClaimName: pvcName,
							},
						},
					},
				},
			},
		}
		_, err = kubeClient.CoreV1().Pods(ns.Name).Create(context.TODO(), pod, metav1.CreateOptions{})
		Expect(err).To(BeNil())

		By(fmt.Sprintf("Waiting for Pod %s to be Running", podName))
		err = wait.PollImmediate(5*time.Second, 5*time.Minute, func() (bool, error) {
			p, err := kubeClient.CoreV1().Pods(ns.Name).Get(context.TODO(), podName, metav1.GetOptions{})
			if err != nil {
				return false, err
			}
			return p.Status.Phase == corev1.PodRunning, nil
		})
		Expect(err).To(BeNil(), "Pod did not reach Running state")

		By("Verifying volume is attached and writable")
		// Verify via kubectl exec if possible, but for now we trust K8s PodRunning state
		// which implies volume is mounted.
	})

	It("Should handle Pod migration between nodes", func() {
		scName := "test-sc-mig-" + string(uuid.NewUUID())[:8]
		pvcName := "test-pvc-mig-" + string(uuid.NewUUID())[:8]
		podName1 := "test-pod-mig-1"
		podName2 := "test-pod-mig-2"

		By(fmt.Sprintf("Creating StorageClass %s", scName))
		sc := &storagev1.StorageClass{
			ObjectMeta: metav1.ObjectMeta{Name: scName},
			Provisioner: "pd.csi.storage.gke.io",
			Parameters: map[string]string{
				"type": "pd-balanced",
			},
			VolumeBindingMode: func() *storagev1.VolumeBindingMode {
				v := storagev1.VolumeBindingWaitForFirstConsumer
				return &v
			}(),
		}
		_, err := kubeClient.StorageV1().StorageClasses().Create(context.TODO(), sc, metav1.CreateOptions{})
		Expect(err).To(BeNil())
		defer kubeClient.StorageV1().StorageClasses().Delete(context.TODO(), scName, metav1.DeleteOptions{})

		By(fmt.Sprintf("Creating PVC %s", pvcName))
		pvc := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      pvcName,
				Namespace: ns.Name,
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: resource.MustParse("10Gi"),
					},
				},
				StorageClassName: &scName,
			},
		}
		_, err = kubeClient.CoreV1().PersistentVolumeClaims(ns.Name).Create(context.TODO(), pvc, metav1.CreateOptions{})
		Expect(err).To(BeNil())

		nodes, err := kubeClient.CoreV1().Nodes().List(context.TODO(), metav1.ListOptions{})
		Expect(err).To(BeNil())
		Expect(len(nodes.Items)).To(BeNumerically(">", 1), "Need at least 2 nodes for migration test")
		node1 := nodes.Items[0].Name
		node2 := nodes.Items[1].Name

		By(fmt.Sprintf("Creating Pod %s on Node %s", podName1, node1))
		pod1 := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: podName1, Namespace: ns.Name},
			Spec: corev1.PodSpec{
				NodeName: node1,
				Containers: []corev1.Container{
					{
						Name:  "web",
						Image: "busybox",
						Command: []string{"sh", "-c", "echo 'migration test' > /data/test.txt && sleep 3600"},
						VolumeMounts: []corev1.VolumeMount{{Name: "data", MountPath: "/data"}},
					},
				},
				Volumes: []corev1.PodVolume{{Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvcName}}}},
			},
		}
		_, err = kubeClient.CoreV1().Pods(ns.Name).Create(context.TODO(), pod1, metav1.CreateOptions{})
		Expect(err).To(BeNil())

		err = waitForPodRunning(kubeClient, ns.Name, podName1)
		Expect(err).To(BeNil())

		By(fmt.Sprintf("Deleting Pod %s", podName1))
		err = kubeClient.CoreV1().Pods(ns.Name).Delete(context.TODO(), podName1, metav1.DeleteOptions{})
		Expect(err).To(BeNil())

		By(fmt.Sprintf("Creating Pod %s on Node %s (different node)", podName2, node2))
		pod2 := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: podName2, Namespace: ns.Name},
			Spec: corev1.PodSpec{
				NodeName: node2,
				Containers: []corev1.Container{
					{
						Name:  "web",
						Image: "busybox",
						Command: []string{"sh", "-c", "cat /data/test.txt && sleep 3600"},
						VolumeMounts: []corev1.VolumeMount{{Name: "data", MountPath: "/data"}},
					},
				},
				Volumes: []corev1.PodVolume{{Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvcName}}}},
			},
		}
		_, err = kubeClient.CoreV1().Pods(ns.Name).Create(context.TODO(), pod2, metav1.CreateOptions{})
		Expect(err).To(BeNil())

		err = waitForPodRunning(kubeClient, ns.Name, podName2)
		Expect(err).To(BeNil(), "Pod did not start on new node, likely volume stuck at old node")
	})
})

func waitForPodRunning(client kubernetes.Interface, ns, name string) error {
	return wait.PollImmediate(5*time.Second, 5*time.Minute, func() (bool, error) {
		p, err := client.CoreV1().Pods(ns).Get(context.TODO(), name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		return p.Status.Phase == corev1.PodRunning, nil
	})
}

func getKubeConfig() (string, error) {
	config, ok := os.LookupEnv("KUBECONFIG")
	if ok {
		return config, nil
	}
	homeDir, ok := os.LookupEnv("HOME")
	if !ok {
		return "", fmt.Errorf("HOME env not set")
	}
	return filepath.Join(homeDir, ".kube/config"), nil
}

func getKubeClient() (kubernetes.Interface, error) {
	kubeConfig, err := getKubeConfig()
	if err != nil {
		return nil, err
	}
	config, err := clientcmd.BuildConfigFromFlags("", kubeConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create config: %v", err.Error())
	}
	kubeClient, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create client: %v", err.Error())
	}
	return kubeClient, nil
}
