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
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/remotecommand"
)

var _ = Describe("K8s GCE PD CSI Driver Autoprovisioning Tests", Label("k8s-autoprovisioning"), func() {
	var (
		kubeClient    kubernetes.Interface
		dynamicClient dynamic.Interface
		restConfig    *rest.Config
		ns            *corev1.Namespace
		//projectID     string
		zone          string
	)

	BeforeEach(func() {
		var err error
		restConfig, kubeClient, err = getKubeConfigAndClient()
		Expect(err).To(BeNil(), "Failed to create kubernetes client")

		dynamicClient, err = dynamic.NewForConfig(restConfig)
		Expect(err).To(BeNil(), "Failed to create dynamic client")

		nsName := "autoprovisioning-test-" + string(uuid.NewUUID())[:8]
		ns, err = kubeClient.CoreV1().Namespaces().Create(context.TODO(), &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: nsName},
		}, metav1.CreateOptions{})
		Expect(err).To(BeNil(), "Failed to create namespace %s", nsName)

		//projectID = *project
		zone = strings.Split(*zones, ",")[0]
	})

	AfterEach(func() {
		if ns != nil && kubeClient != nil {
			By(fmt.Sprintf("Deleting namespace %s", ns.Name))
			_ = kubeClient.CoreV1().Namespaces().Delete(context.TODO(), ns.Name, metav1.DeleteOptions{})
		}
	})

	It("Should provision a volume and attach it to a pod with node affinity", func() {
		scName := "test-sc-" + string(uuid.NewUUID())[:8]
		pvcName := "test-pvc-" + string(uuid.NewUUID())[:8]
		podName := "test-pod-" + string(uuid.NewUUID())[:8]

		By(fmt.Sprintf("Creating StorageClass %s", scName))
		createStorageClass(kubeClient, scName, "pd-balanced", true)
		defer kubeClient.StorageV1().StorageClasses().Delete(context.TODO(), scName, metav1.DeleteOptions{})

		By(fmt.Sprintf("Creating PVC %s", pvcName))
		createPVC(kubeClient, ns.Name, pvcName, scName, "10Gi")

		By("Identifying a target node")
		nodes, err := kubeClient.CoreV1().Nodes().List(context.TODO(), metav1.ListOptions{})
		Expect(err).To(BeNil())
		Expect(len(nodes.Items)).To(BeNumerically(">", 0))
		targetNode := nodes.Items[0].Name

		By(fmt.Sprintf("Creating Pod %s on node %s", podName, targetNode))
		createPod(kubeClient, ns.Name, podName, pvcName, targetNode, "echo 'hello' > /data/test.log && sleep 3600")

		By(fmt.Sprintf("Waiting for Pod %s to be Running", podName))
		err = waitForPodRunning(kubeClient, ns.Name, podName)
		Expect(err).To(BeNil(), "Pod did not reach Running state")

		By("Verifying volume is writable")
		stdout, stderr, err := execCommandInPod(kubeClient, restConfig, ns.Name, podName, []string{"cat", "/data/test.log"})
		Expect(err).To(BeNil(), "Exec failed: %s", stderr)
		Expect(strings.TrimSpace(stdout)).To(Equal("hello"))
	})

	It("Should handle Pod migration between nodes", func() {
		scName := "test-sc-mig-" + string(uuid.NewUUID())[:8]
		pvcName := "test-pvc-mig-" + string(uuid.NewUUID())[:8]
		podName1 := "test-pod-mig-1"
		podName2 := "test-pod-mig-2"

		By("Checking node count")
		nodes, err := kubeClient.CoreV1().Nodes().List(context.TODO(), metav1.ListOptions{})
		Expect(err).To(BeNil())
		
		var node1, node2 string
		for _, node := range nodes.Items {
			if node.Labels["topology.gke.io/zone"] == zone {
				if node1 == "" {
					node1 = node.Name
				} else if node2 == "" {
					node2 = node.Name
				}
			}
		}

		if node1 == "" || node2 == "" {
			Skip(fmt.Sprintf("Need at least 2 nodes in zone %s for migration test", zone))
		}

		createStorageClass(kubeClient, scName, "pd-balanced", true)
		defer kubeClient.StorageV1().StorageClasses().Delete(context.TODO(), scName, metav1.DeleteOptions{})

		createPVC(kubeClient, ns.Name, pvcName, scName, "10Gi")

		By(fmt.Sprintf("Creating Pod %s on Node %s", podName1, node1))
		createPod(kubeClient, ns.Name, podName1, pvcName, node1, "echo 'migration test' > /data/test.txt && sleep 3600")
		Expect(waitForPodRunning(kubeClient, ns.Name, podName1)).To(BeNil())

		By(fmt.Sprintf("Deleting Pod %s", podName1))
		err = kubeClient.CoreV1().Pods(ns.Name).Delete(context.TODO(), podName1, metav1.DeleteOptions{})
		Expect(err).To(BeNil())
		
		By("Waiting for volume to detach (Pod deletion doesn't guarantee detach immediately, but scheduling new pod will force it)")

		By(fmt.Sprintf("Creating Pod %s on Node %s (different node)", podName2, node2))
		createPod(kubeClient, ns.Name, podName2, pvcName, node2, "cat /data/test.txt && sleep 3600")
		
		By("Waiting for second pod to start (implies successful detach/attach)")
		Expect(waitForPodRunning(kubeClient, ns.Name, podName2)).To(BeNil())
		
		By("Verifying data on new node")
		stdout, _, err := execCommandInPod(kubeClient, restConfig, ns.Name, podName2, []string{"cat", "/data/test.txt"})
		Expect(err).To(BeNil())
		Expect(strings.TrimSpace(stdout)).To(Equal("migration test"))
	})

	It("Should resize a volume online (Capacity Expansion)", func() {
		scName := "test-sc-resize-" + string(uuid.NewUUID())[:8]
		pvcName := "test-pvc-resize-" + string(uuid.NewUUID())[:8]
		podName := "test-pod-resize"

		By("Creating StorageClass with allowVolumeExpansion: true")
		sc := &storagev1.StorageClass{
			ObjectMeta: metav1.ObjectMeta{Name: scName},
			Provisioner: "pd.csi.storage.gke.io",
			Parameters: map[string]string{"type": "pd-balanced"},
			AllowVolumeExpansion: func() *bool { t := true; return &t }(),
			VolumeBindingMode: func() *storagev1.VolumeBindingMode { v := storagev1.VolumeBindingWaitForFirstConsumer; return &v }(),
		}
		_, err := kubeClient.StorageV1().StorageClasses().Create(context.TODO(), sc, metav1.CreateOptions{})
		Expect(err).To(BeNil())
		defer kubeClient.StorageV1().StorageClasses().Delete(context.TODO(), scName, metav1.DeleteOptions{})

		By("Creating PVC 10Gi")
		createPVC(kubeClient, ns.Name, pvcName, scName, "10Gi")

		By("Creating Pod")
		createPod(kubeClient, ns.Name, podName, pvcName, "", "while true; do echo data >> /data/out.log; sleep 5; done")
		Expect(waitForPodRunning(kubeClient, ns.Name, podName)).To(BeNil())

		By("Verifying initial size (approx 10Gi)")
		// Use df -BG to get size in GB
		stdout, _, err := execCommandInPod(kubeClient, restConfig, ns.Name, podName, []string{"df", "-BG", "/data"})
		Expect(err).To(BeNil())
		Expect(stdout).To(MatchRegexp(`\s10\s`), "Initial size should be 10G")

		By(" expanding PVC to 20Gi")
		pvc, err := kubeClient.CoreV1().PersistentVolumeClaims(ns.Name).Get(context.TODO(), pvcName, metav1.GetOptions{})
		Expect(err).To(BeNil())
		pvc.Spec.Resources.Requests[corev1.ResourceStorage] = resource.MustParse("20Gi")
		_, err = kubeClient.CoreV1().PersistentVolumeClaims(ns.Name).Update(context.TODO(), pvc, metav1.UpdateOptions{})
		Expect(err).To(BeNil())

		By("Waiting for PVC status capacity to update")
		err = wait.PollImmediate(5*time.Second, 5*time.Minute, func() (bool, error) {
			p, err := kubeClient.CoreV1().PersistentVolumeClaims(ns.Name).Get(context.TODO(), pvcName, metav1.GetOptions{})
			if err != nil { return false, err }
			q := p.Status.Capacity[corev1.ResourceStorage]
			return q.Cmp(resource.MustParse("20Gi")) == 0, nil
		})
		Expect(err).To(BeNil(), "PVC status capacity did not update")

		By("Waiting for file system resize in Pod")
		err = wait.PollImmediate(5*time.Second, 5*time.Minute, func() (bool, error) {
			stdout, _, err := execCommandInPod(kubeClient, restConfig, ns.Name, podName, []string{"df", "-BG", "/data"})
			if err != nil { return false, nil }
			if strings.Contains(stdout, " 20 ") {
				return true, nil
			}
			return false, nil
		})
		Expect(err).To(BeNil(), "File system size did not update to 20G in pod")
	})

	It("Should snapshot and restore a volume (Data Integrity)", func() {
		if !checkSnapshotCRD(dynamicClient) {
			Skip("VolumeSnapshot CRDs not installed, skipping snapshot integrity test")
		}
		scName := "test-sc-snap-" + string(uuid.NewUUID())[:8]
		vscName := "test-vsc-snap-" + string(uuid.NewUUID())[:8]
		pvc1Name := "pvc-source"
		pvc2Name := "pvc-restore"
		pod1Name := "pod-source"
		pod2Name := "pod-restore"
		snapName := "test-snapshot"

		createStorageClass(kubeClient, scName, "pd-balanced", true)
		defer kubeClient.StorageV1().StorageClasses().Delete(context.TODO(), scName, metav1.DeleteOptions{})

		By("Creating VolumeSnapshotClass")
		createVolumeSnapshotClass(dynamicClient, vscName)
		defer deleteVolumeSnapshotClass(dynamicClient, vscName)

		By("Creating Source PVC and Pod")
		createPVC(kubeClient, ns.Name, pvc1Name, scName, "10Gi")
		createPod(kubeClient, ns.Name, pod1Name, pvc1Name, "", "echo 'vital data' > /data/data.txt && sleep 3600")
		Expect(waitForPodRunning(kubeClient, ns.Name, pod1Name)).To(BeNil())
		
		// verify data existence
		_, _, err := execCommandInPod(kubeClient, restConfig, ns.Name, pod1Name, []string{"sync"})
		Expect(err).To(BeNil())

		By("Creating VolumeSnapshot")
		createVolumeSnapshot(dynamicClient, ns.Name, snapName, vscName, pvc1Name)
		defer deleteVolumeSnapshot(dynamicClient, ns.Name, snapName)

		By("Waiting for Snapshot to be ReadyToUse")
		err = waitForSnapshotReady(dynamicClient, ns.Name, snapName)
		Expect(err).To(BeNil(), "Snapshot failed to become ReadyToUse")

		By("Creating Restored PVC from Snapshot")
		createPVCFromSnapshot(kubeClient, ns.Name, pvc2Name, scName, "10Gi", snapName)

		By("Creating Pod using Restored PVC")
		createPod(kubeClient, ns.Name, pod2Name, pvc2Name, "", "cat /data/data.txt && sleep 3600")
		Expect(waitForPodRunning(kubeClient, ns.Name, pod2Name)).To(BeNil())

		By("Verifying data in Restored Pod")
		stdout, _, err := execCommandInPod(kubeClient, restConfig, ns.Name, pod2Name, []string{"cat", "/data/data.txt"})
		Expect(err).To(BeNil())
		Expect(strings.TrimSpace(stdout)).To(Equal("vital data"))
	})

	It("Should recover and reattach volume after node recreation (Ghost Attachment simulation)", func() {
		scName := "test-sc-ghost-" + string(uuid.NewUUID())[:8]
		pvcName := "test-pvc-ghost-" + string(uuid.NewUUID())[:8]
		podName1 := "test-pod-ghost-1"
		podName2 := "test-pod-ghost-2"

		nodes, err := kubeClient.CoreV1().Nodes().List(context.TODO(), metav1.ListOptions{})
		Expect(err).To(BeNil())
		
		var node1, node2 string
		currentNode := os.Getenv("NODE_NAME")
		if currentNode == "" {
			currentNode, _ = os.Hostname()
		}

		for _, node := range nodes.Items {
			if node.Name != currentNode && node.Name != "csi" && node.Labels["topology.gke.io/zone"] == zone {
				if node1 == "" {
					node1 = node.Name
				} else if node2 == "" {
					node2 = node.Name
				}
			}
		}
		
		if node1 == "" {
			Skip(fmt.Sprintf("Could not find a worker node to delete in zone %s", zone))
		}
		if node2 == "" {
			for _, node := range nodes.Items {
				if node.Name != node1 && node.Labels["topology.gke.io/zone"] == zone {
					node2 = node.Name
					break
				}
			}
		}
		if node2 == "" {
			Skip(fmt.Sprintf("Could not find a second node in zone %s for migration", zone))
		}

		createStorageClass(kubeClient, scName, "pd-balanced", true)
		defer kubeClient.StorageV1().StorageClasses().Delete(context.TODO(), scName, metav1.DeleteOptions{})

		By("Creating PVC and Pod on Node 1")
		createPVC(kubeClient, ns.Name, pvcName, scName, "10Gi")
		createPod(kubeClient, ns.Name, podName1, pvcName, node1, "echo 'ghost data' > /data/ghost.txt && sleep 3600")
		Expect(waitForPodRunning(kubeClient, ns.Name, podName1)).To(BeNil())

		By(fmt.Sprintf("Abruptly deleting Node 1 VM: %s", node1))
		// We use gcloud to delete the instance. This assumes the test runs in an env where gcloud is auth'd
		// and matches the cluster.
		cmd := exec.Command("gcloud", "compute", "instances", "delete", node1, "--zone", zone, "--quiet")
		out, err := cmd.CombinedOutput()
		Expect(err).To(BeNil(), "Failed to delete instance %s: %s", node1, out)

		By("Waiting for Node 1 to be NotReady in K8s")
		err = wait.PollImmediate(5*time.Second, 2*time.Minute, func() (bool, error) {
			n, err := kubeClient.CoreV1().Nodes().Get(context.TODO(), node1, metav1.GetOptions{})
			if err != nil { return false, nil } // API might fail transiently
			for _, cond := range n.Status.Conditions {
				if cond.Type == corev1.NodeReady && cond.Status != corev1.ConditionTrue {
					return true, nil
				}
			}
			return false, nil
		})		
		By("Force deleting Pod 1 (since node is dead, it won't terminate gracefully)")
		grace := int64(0)
		err = kubeClient.CoreV1().Pods(ns.Name).Delete(context.TODO(), podName1, metav1.DeleteOptions{GracePeriodSeconds: &grace})
		Expect(err).To(BeNil())

		By(fmt.Sprintf("Creating Pod 2 on Node 2: %s", node2))
		createPod(kubeClient, ns.Name, podName2, pvcName, node2, "cat /data/ghost.txt && sleep 3600")
		
		By("Waiting for Pod 2 to start (Driver must detect detach from dead node and attach to new node)")
		err = wait.PollImmediate(10*time.Second, 10*time.Minute, func() (bool, error) {
			p, err := kubeClient.CoreV1().Pods(ns.Name).Get(context.TODO(), podName2, metav1.GetOptions{})
			if err != nil { return false, err }
			if p.Status.Phase == corev1.PodRunning { return true, nil }
			// Optional: check events for "Multi-Attach" errors to see if it's struggling
			return false, nil
		})
		Expect(err).To(BeNil(), "Pod 2 did not start on new node after old node death")

		By("Verifying data")
		stdout, _, err := execCommandInPod(kubeClient, restConfig, ns.Name, podName2, []string{"cat", "/data/ghost.txt"})
		Expect(err).To(BeNil())
		Expect(strings.TrimSpace(stdout)).To(Equal("ghost data"))
	})

	It("Should clone a volume and verify data integrity (PVC-to-PVC Clone)", func() {
		scName := "test-sc-clone-" + string(uuid.NewUUID())[:8]
		pvcSrcName := "pvc-src-" + string(uuid.NewUUID())[:8]
		pvcCloneName := "pvc-clone-" + string(uuid.NewUUID())[:8]
		podSrcName := "pod-src"
		podCloneName := "pod-clone"

		createStorageClass(kubeClient, scName, "pd-balanced", true)
		defer kubeClient.StorageV1().StorageClasses().Delete(context.TODO(), scName, metav1.DeleteOptions{})

		By("Creating Source PVC and Pod")
		createPVC(kubeClient, ns.Name, pvcSrcName, scName, "10Gi")
		createPod(kubeClient, ns.Name, podSrcName, pvcSrcName, "", "echo 'original data' > /data/file.txt && sync && sleep 3600")
		Expect(waitForPodRunning(kubeClient, ns.Name, podSrcName)).To(BeNil())

		By("Deleting Source Pod to ensure volume is detached for cloning (GCE PD requirement for some cases)")
		err := kubeClient.CoreV1().Pods(ns.Name).Delete(context.TODO(), podSrcName, metav1.DeleteOptions{})
		Expect(err).To(BeNil())

		By("Creating Cloned PVC from Source PVC")
		createPVCClone(kubeClient, ns.Name, pvcCloneName, scName, "10Gi", pvcSrcName)

		By("Creating Pod using Cloned PVC")
		createPod(kubeClient, ns.Name, podCloneName, pvcCloneName, "", "cat /data/file.txt && echo 'cloned update' >> /data/file.txt && sleep 3600")
		Expect(waitForPodRunning(kubeClient, ns.Name, podCloneName)).To(BeNil())

		By("Verifying data in Cloned Pod")
		stdout, _, err := execCommandInPod(kubeClient, restConfig, ns.Name, podCloneName, []string{"cat", "/data/file.txt"})
		Expect(err).To(BeNil())
		Expect(strings.TrimSpace(stdout)).To(ContainSubstring("original data"))

		By("Verifying the clone is independent (Source data should NOT have 'cloned update')")
		createPod(kubeClient, ns.Name, podSrcName+"-verify", pvcSrcName, "", "cat /data/file.txt && sleep 3600")
		Expect(waitForPodRunning(kubeClient, ns.Name, podSrcName+"-verify")).To(BeNil())
		stdout, _, err = execCommandInPod(kubeClient, restConfig, ns.Name, podSrcName+"-verify", []string{"cat", "/data/file.txt"})
		Expect(err).To(BeNil())
		Expect(strings.TrimSpace(stdout)).To(Equal("original data"))
	})

	It("Should support ReadOnlyMany (ROX) across multiple nodes", func() {
		if !checkSnapshotCRD(dynamicClient) {
			Skip("VolumeSnapshot CRDs not installed, skipping ROX multinode test")
		}
		scName := "test-sc-rox-" + string(uuid.NewUUID())[:8]
		vscName := "test-vsc-rox-" + string(uuid.NewUUID())[:8]
		pvcSrcName := "pvc-src-rox"
		pvcRoxName := "pvc-rox"
		snapName := "snap-rox"

		By("Checking node count and distribution")
		nodes, err := kubeClient.CoreV1().Nodes().List(context.TODO(), metav1.ListOptions{})
		Expect(err).To(BeNil())
		
		var node1, node2 string
		for _, node := range nodes.Items {
			if node.Labels["topology.gke.io/zone"] == zone {
				if node1 == "" {
					node1 = node.Name
				} else if node2 == "" {
					node2 = node.Name
				}
			}
		}
		if node1 == "" || node2 == "" {
			Skip("Need at least 2 nodes in the same zone for ROX multinode test")
		}

		createStorageClass(kubeClient, scName, "pd-balanced", true)
		defer kubeClient.StorageV1().StorageClasses().Delete(context.TODO(), scName, metav1.DeleteOptions{})
		createVolumeSnapshotClass(dynamicClient, vscName)
		defer deleteVolumeSnapshotClass(dynamicClient, vscName)

		By("Preparing data on a RWO volume")
		createPVC(kubeClient, ns.Name, pvcSrcName, scName, "10Gi")
		createPod(kubeClient, ns.Name, "pod-writer", pvcSrcName, node1, "echo 'shared RO data' > /data/shared.txt && sync && sleep 3600")
		Expect(waitForPodRunning(kubeClient, ns.Name, "pod-writer")).To(BeNil())
		
		By("Creating Snapshot of the data")
		createVolumeSnapshot(dynamicClient, ns.Name, snapName, vscName, pvcSrcName)
		Expect(waitForSnapshotReady(dynamicClient, ns.Name, snapName)).To(BeNil())

		By("Creating ROX PVC from Snapshot")
		createROXPVCFromSnapshot(kubeClient, ns.Name, pvcRoxName, scName, "10Gi", snapName)

		By(fmt.Sprintf("Creating two reader pods on different nodes: %s, %s", node1, node2))
		podReader1 := "pod-reader-1"
		podReader2 := "pod-reader-2"
		createPod(kubeClient, ns.Name, podReader1, pvcRoxName, node1, "while true; do cat /data/shared.txt; sleep 10; done")
		createPod(kubeClient, ns.Name, podReader2, pvcRoxName, node2, "while true; do cat /data/shared.txt; sleep 10; done")

		By("Waiting for both reader pods to be running")
		Expect(waitForPodRunning(kubeClient, ns.Name, podReader1)).To(BeNil())
		Expect(waitForPodRunning(kubeClient, ns.Name, podReader2)).To(BeNil())

		By("Verifying data readability from both pods")
		for _, pod := range []string{podReader1, podReader2} {
			stdout, _, err := execCommandInPod(kubeClient, restConfig, ns.Name, pod, []string{"cat", "/data/shared.txt"})
			Expect(err).To(BeNil())
			Expect(strings.TrimSpace(stdout)).To(Equal("shared RO data"))
		}

		By("Verifying that writes are prohibited in ROX pods")
		for _, pod := range []string{podReader1, podReader2} {
			_, stderr, err := execCommandInPod(kubeClient, restConfig, ns.Name, pod, []string{"touch", "/data/illegal-write"})
			// touch should fail on a read-only filesystem
			Expect(err).NotTo(BeNil(), "Write should have failed in pod %s", pod)
			Expect(stderr).To(ContainSubstring("Read-only file system"))
		}
	})

	It("Should restore a snapshot to a larger volume and verify filesystem resize", func() {
		if !checkSnapshotCRD(dynamicClient) {
			Skip("VolumeSnapshot CRDs not installed, skipping restore-to-larger test")
		}
		scName := "test-sc-resnap-" + string(uuid.NewUUID())[:8]
		vscName := "test-vsc-resnap-" + string(uuid.NewUUID())[:8]
		pvcSrcName := "pvc-src-resnap"
		pvcLargeName := "pvc-large-resnap"
		snapName := "snap-resnap"

		createStorageClass(kubeClient, scName, "pd-balanced", true)
		defer kubeClient.StorageV1().StorageClasses().Delete(context.TODO(), scName, metav1.DeleteOptions{})
		createVolumeSnapshotClass(dynamicClient, vscName)
		defer deleteVolumeSnapshotClass(dynamicClient, vscName)

		By("Creating 10Gi Source PVC")
		createPVC(kubeClient, ns.Name, pvcSrcName, scName, "10Gi")
		createPod(kubeClient, ns.Name, "pod-src-resnap", pvcSrcName, "", "echo 'resize data' > /data/resize.txt && sync && sleep 3600")
		Expect(waitForPodRunning(kubeClient, ns.Name, "pod-src-resnap")).To(BeNil())

		By("Taking Snapshot")
		createVolumeSnapshot(dynamicClient, ns.Name, snapName, vscName, pvcSrcName)
		Expect(waitForSnapshotReady(dynamicClient, ns.Name, snapName)).To(BeNil())

		By("Restoring to a 20Gi PVC")
		createPVCFromSnapshot(kubeClient, ns.Name, pvcLargeName, scName, "20Gi", snapName)

		By("Creating Pod with 20Gi restored volume")
		createPod(kubeClient, ns.Name, "pod-large-resnap", pvcLargeName, "", "sleep 3600")
		Expect(waitForPodRunning(kubeClient, ns.Name, "pod-large-resnap")).To(BeNil())

		By("Verifying data integrity")
		stdout, _, err := execCommandInPod(kubeClient, restConfig, ns.Name, "pod-large-resnap", []string{"cat", "/data/resize.txt"})
		Expect(err).To(BeNil())
		Expect(strings.TrimSpace(stdout)).To(Equal("resize data"))

		By("Verifying filesystem size is 20Gi")
		stdout, _, err = execCommandInPod(kubeClient, restConfig, ns.Name, "pod-large-resnap", []string{"df", "-BG", "/data"})
		Expect(err).To(BeNil())
		Expect(stdout).To(MatchRegexp(`\s20\s`), "Filesystem should be 20G after restore-to-larger")
	})
})

func getKubeConfigAndClient() (*rest.Config, kubernetes.Interface, error) {
	configStr, ok := os.LookupEnv("KUBECONFIG")
	if !ok {
		homeDir, _ := os.LookupEnv("HOME")
		configStr = filepath.Join(homeDir, ".kube/config")
	}
	config, err := clientcmd.BuildConfigFromFlags("", configStr)
	if err != nil { return nil, nil, err }
	client, err := kubernetes.NewForConfig(config)
	return config, client, err
}

func createStorageClass(client kubernetes.Interface, name, diskType string, waitForFirstConsumer bool) {
	mode := storagev1.VolumeBindingImmediate
	if waitForFirstConsumer {
		mode = storagev1.VolumeBindingWaitForFirstConsumer
	}
	sc := &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Provisioner: "pd.csi.storage.gke.io",
		Parameters: map[string]string{"type": diskType},
		VolumeBindingMode: &mode,
		ReclaimPolicy: func() *corev1.PersistentVolumeReclaimPolicy { p := corev1.PersistentVolumeReclaimDelete; return &p }(),
	}
	_, err := client.StorageV1().StorageClasses().Create(context.TODO(), sc, metav1.CreateOptions{})
	Expect(err).To(BeNil())
}

func createPVC(client kubernetes.Interface, ns, name, scName, size string) {
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse(size)}},
			StorageClassName: &scName,
		},
	}
	_, err := client.CoreV1().PersistentVolumeClaims(ns).Create(context.TODO(), pvc, metav1.CreateOptions{})
	Expect(err).To(BeNil())
}

func createPVCClone(client kubernetes.Interface, ns, name, scName, size, sourcePVCName string) {
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse(size)}},
			StorageClassName: &scName,
			DataSource: &corev1.TypedLocalObjectReference{
				Kind: "PersistentVolumeClaim",
				Name: sourcePVCName,
			},
		},
	}
	_, err := client.CoreV1().PersistentVolumeClaims(ns).Create(context.TODO(), pvc, metav1.CreateOptions{})
	Expect(err).To(BeNil())
}

func createROXPVCFromSnapshot(client kubernetes.Interface, ns, name, scName, size, snapshotName string) {
	apiGroup := "snapshot.storage.k8s.io"
	kind := "VolumeSnapshot"
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadOnlyMany},
			Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse(size)}},
			StorageClassName: &scName,
			DataSource: &corev1.TypedLocalObjectReference{
				APIGroup: &apiGroup,
				Kind:     kind,
				Name:     snapshotName,
			},
		},
	}
	_, err := client.CoreV1().PersistentVolumeClaims(ns).Create(context.TODO(), pvc, metav1.CreateOptions{})
	Expect(err).To(BeNil())
}

func createPod(client kubernetes.Interface, ns, name, pvcName, nodeName, cmd string) {
    pod := &corev1.Pod{
        ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
        Spec: corev1.PodSpec{
            Containers: []corev1.Container{{
                Name:    "web",
                Image:   "busybox",
                Command: []string{"sh", "-c", cmd},
                VolumeMounts: []corev1.VolumeMount{{
                    Name:      "data",
                    MountPath: "/data",
                }},
            }},
            Volumes: []corev1.Volume{{
                Name: "data",
                VolumeSource: corev1.VolumeSource{
                    PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
                        ClaimName: pvcName,
                    },
                },
            }},
        },
    }
    if nodeName != "" {
        pod.Spec.NodeSelector = map[string]string{"kubernetes.io/hostname": nodeName}
    }

    _, err := client.CoreV1().Pods(ns).Create(context.TODO(), pod, metav1.CreateOptions{})
    Expect(err).To(BeNil())
}

func waitForPodRunning(client kubernetes.Interface, ns, name string) error {
	return wait.PollImmediate(5*time.Second, 5*time.Minute, func() (bool, error) {
		p, err := client.CoreV1().Pods(ns).Get(context.TODO(), name, metav1.GetOptions{})
		if err != nil { return false, err }
		return p.Status.Phase == corev1.PodRunning, nil
	})
}

func execCommandInPod(client kubernetes.Interface, config *rest.Config, ns, podName string, cmd []string) (string, string, error) {
	req := client.CoreV1().RESTClient().Post().Resource("pods").Name(podName).Namespace(ns).SubResource("exec")
	option := &corev1.PodExecOptions{Command: cmd, Stdin: false, Stdout: true, Stderr: true, TTY: false}
	req.VersionedParams(option, scheme.ParameterCodec)
	exec, err := remotecommand.NewSPDYExecutor(config, "POST", req.URL())
	if err != nil { return "", "", err }
	var stdout, stderr bytes.Buffer
	err = exec.Stream(remotecommand.StreamOptions{Stdout: &stdout, Stderr: &stderr})
	return stdout.String(), stderr.String(), err
}

func checkSnapshotCRD(client dynamic.Interface) bool {
	gvr := schema.GroupVersionResource{Group: "apiextensions.k8s.io", Version: "v1", Resource: "customresourcedefinitions"}
	_, err := client.Resource(gvr).Get(context.TODO(), "volumesnapshots.snapshot.storage.k8s.io", metav1.GetOptions{})
	return err == nil
}

// Snapshot Helpers using Dynamic Client

var snapshotGVR = schema.GroupVersionResource{Group: "snapshot.storage.k8s.io", Version: "v1", Resource: "volumesnapshots"}
var snapshotClassGVR = schema.GroupVersionResource{Group: "snapshot.storage.k8s.io", Version: "v1", Resource: "volumesnapshotclasses"}

func createVolumeSnapshotClass(client dynamic.Interface, name string) {
	obj := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "snapshot.storage.k8s.io/v1",
			"kind":       "VolumeSnapshotClass",
			"metadata":   map[string]interface{}{"name": name},
			"driver":     "pd.csi.storage.gke.io",
			"deletionPolicy": "Delete",
		},
	}
	_, err := client.Resource(snapshotClassGVR).Create(context.TODO(), obj, metav1.CreateOptions{})
	Expect(err).To(BeNil())
}

func deleteVolumeSnapshotClass(client dynamic.Interface, name string) {
	_ = client.Resource(snapshotClassGVR).Delete(context.TODO(), name, metav1.DeleteOptions{})
}

func createVolumeSnapshot(client dynamic.Interface, ns, name, className, pvcName string) {
	obj := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "snapshot.storage.k8s.io/v1",
			"kind":       "VolumeSnapshot",
			"metadata":   map[string]interface{}{"name": name, "namespace": ns},
			"spec": map[string]interface{}{
				"volumeSnapshotClassName": className,
				"source": map[string]interface{}{"persistentVolumeClaimName": pvcName},
			},
		},
	}
	_, err := client.Resource(snapshotGVR).Namespace(ns).Create(context.TODO(), obj, metav1.CreateOptions{})
	Expect(err).To(BeNil())
}

func deleteVolumeSnapshot(client dynamic.Interface, ns, name string) {
	_ = client.Resource(snapshotGVR).Namespace(ns).Delete(context.TODO(), name, metav1.DeleteOptions{})
}

func waitForSnapshotReady(client dynamic.Interface, ns, name string) error {
	return wait.PollImmediate(5*time.Second, 10*time.Minute, func() (bool, error) {
		obj, err := client.Resource(snapshotGVR).Namespace(ns).Get(context.TODO(), name, metav1.GetOptions{})
		if err != nil { return false, err }
		status, found, _ := unstructured.NestedMap(obj.Object, "status")
		if !found { return false, nil }
		ready, found, _ := unstructured.NestedBool(status, "readyToUse")
		return found && ready, nil
	})
}

func createPVCFromSnapshot(client kubernetes.Interface, ns, name, scName, size, snapshotName string) {
	apiGroup := "snapshot.storage.k8s.io"
	kind := "VolumeSnapshot"
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse(size)}},
			StorageClassName: &scName,
			DataSource: &corev1.TypedLocalObjectReference{
				APIGroup: &apiGroup,
				Kind:     kind,
				Name:     snapshotName,
			},
		},
	}
	_, err := client.CoreV1().PersistentVolumeClaims(ns).Create(context.TODO(), pvc, metav1.CreateOptions{})
	Expect(err).To(BeNil())
}
