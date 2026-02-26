/*
Copyright 2025 The Kubernetes Authors.

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
	"fmt"
	"sync"
	"time"

	csi "github.com/container-storage-interface/spec/lib/go/csi"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2"
	"sigs.k8s.io/gcp-compute-persistent-disk-csi-driver/pkg/common"
	"sigs.k8s.io/gcp-compute-persistent-disk-csi-driver/pkg/constants"
	gce "sigs.k8s.io/gcp-compute-persistent-disk-csi-driver/pkg/gce-cloud-provider/compute"
	"sigs.k8s.io/gcp-compute-persistent-disk-csi-driver/pkg/parameters"
	testutils "sigs.k8s.io/gcp-compute-persistent-disk-csi-driver/test/e2e/utils"
	remote "sigs.k8s.io/gcp-compute-persistent-disk-csi-driver/test/remote"
)

var _ = Describe("GCE PD CSI Driver Autoscaling Tests", Label("autoscaling"), func() {
	var (
		p, z, nodeID string
		tc           *remote.TestContext
	)

	BeforeEach(func() {
		Expect(testContexts).ToNot(BeEmpty())
		tc = getRandomTestContext()
		p, z, _ = tc.Instance.GetIdentity()
		nodeID = tc.Instance.GetNodeID()
	})

	It("Should handle volume cleanup and re-attachment after node deletion (Scale-Down scenario)", func() {
		// This test simulates a node being deleted by the cluster autoscaler while a volume is still attached.
		// The CSI driver should allow the volume to be attached to a new node.

		By("Creating a volume")
		volName := testNamePrefix + string(uuid.NewUUID())
		resp, err := tc.Client.CreateVolume(volName, nil, defaultSizeGb, nil, nil)
		Expect(err).To(BeNil(), "CreateVolume failed")
		volID := resp.GetVolumeId()
		defer func() {
			if volID != "" {
				_ = tc.Client.DeleteVolume(volID)
			}
		}()

		By("Provisioning a temporary instance to simulate a node that will be scaled down")
		tempNodeName := fmt.Sprintf("%s-scaledown-%s", testNamePrefix, string(uuid.NewUUID())[:8])
		instanceConfig := remote.InstanceConfig{
			Project:                   p,
			Architecture:              *architecture,
			MinCpuPlatform:            *minCpuPlatform,
			Zone:                      z,
			Name:                      tempNodeName,
			MachineType:               *machineType,
			ServiceAccount:            *serviceAccount,
			ImageURL:                  *imageURL,
			CloudtopHost:              *cloudtopHost,
			EnableConfidentialCompute: *enableConfidentialCompute,
			ComputeService:            computeService,
		}
		tempInstance, err := remote.SetupInstance(instanceConfig)
		Expect(err).To(BeNil(), "Failed to setup temp instance")

		tempTC, err := testutils.GCEClientAndDriverSetup(tempInstance, getDriverConfig())
		Expect(err).To(BeNil(), "Failed to setup CSI driver on temp instance")

		By(fmt.Sprintf("Attaching volume to temporary instance %s", tempNodeName))
		err = tempTC.Client.ControllerPublishVolumeReadWrite(volID, tempInstance.GetNodeID(), false /* forceAttach */)
		Expect(err).To(BeNil(), "ControllerPublishVolume failed on temp node")

		By("Deleting the temporary instance without a clean unpublish (simulating rapid scale-down/crash)")
		tempInstance.DeleteInstance()

		By("Attempting to re-attach the volume to a different node")
		// GCE should allow attachment to a new node once the old instance is deleted.

		Eventually(func() error {
			return tc.Client.ControllerPublishVolumeReadWrite(volID, nodeID, false)
		}, 2*time.Minute, 5*time.Second).Should(BeNil(), "Failed to attach volume to new node after original node was deleted")

		By("Cleaning up the attachment")
		err = tc.Client.ControllerUnpublishVolume(volID, nodeID)
		Expect(err).To(BeNil(), "ControllerUnpublishVolume failed")
	})

	It("Should handle rapid concurrent volume attachments to multiple nodes (Scale-Up stress)", func() {
		// This test simulates a large scale-up where multiple nodes come up and many volumes are attached simultaneously.

		if len(testContexts) < 2 {
			Skip("Need at least two nodes for concurrent attachment test")
		}

		numVolumes := 6
		var wg sync.WaitGroup
		volIDs := make([]string, numVolumes)
		errs := make([]error, numVolumes)

		By(fmt.Sprintf("Creating %d volumes concurrently", numVolumes))
		for i := 0; i < numVolumes; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				defer GinkgoRecover()
				vName := fmt.Sprintf("%s-stress-%d-%s", testNamePrefix, idx, string(uuid.NewUUID())[:8])
				resp, err := tc.Client.CreateVolume(vName, nil, defaultSizeGb, nil, nil)
				if err != nil {
					errs[idx] = err
					return
				}
				volIDs[idx] = resp.GetVolumeId()
			}(i)
		}
		wg.Wait()

		for _, err := range errs {
			Expect(err).To(BeNil(), "Concurrent CreateVolume failed")
		}

		defer func() {
			for _, vid := range volIDs {
				if vid != "" {
					_ = tc.Client.DeleteVolume(vid)
				}
			}
		}()

		By(fmt.Sprintf("Attaching %d volumes to multiple nodes concurrently", numVolumes))
		for i := 0; i < numVolumes; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				defer GinkgoRecover()
				targetTC := testContexts[idx%len(testContexts)]
				err := targetTC.Client.ControllerPublishVolumeReadWrite(volIDs[idx], targetTC.Instance.GetNodeID(), false /* forceAttach */)
				errs[idx] = err
			}(i)
		}
		wg.Wait()

		for i, err := range errs {
			Expect(err).To(BeNil(), "Concurrent attachment failed for volume %d: %v", i, err)
		}

		By("Detaching volumes concurrently")
		for i := 0; i < numVolumes; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				defer GinkgoRecover()
				targetTC := testContexts[idx%len(testContexts)]
				err := targetTC.Client.ControllerUnpublishVolume(volIDs[idx], targetTC.Instance.GetNodeID())
				errs[idx] = err
			}(i)
		}
		wg.Wait()

		for _, err := range errs {
			Expect(err).To(BeNil(), "Concurrent detachment failed")
		}
	})

	It("Should handle regional PD failover during unclean node shutdown (Zone failure scenario)", func() {
		// This test simulates a zone failure where a node in Zone A is lost, and the RePD must be attached to Zone B.

		By("Identifying a region with at least 2 unique zones and instances")
		regionMap := make(map[string]map[string]bool)
		for _, ctx := range testContexts {
			_, zone, _ := ctx.Instance.GetIdentity()
			reg, _ := common.GetRegionFromZones([]string{zone})
			if _, ok := regionMap[reg]; !ok {
				regionMap[reg] = make(map[string]bool)
			}
			regionMap[reg][zone] = true
		}

		var selectedRegion string
		var zones []string
		for r, zsMap := range regionMap {
			if len(zsMap) >= 2 {
				selectedRegion = r
				for z := range zsMap {
					zones = append(zones, z)
				}
				break
			}
		}

		if selectedRegion == "" {
			Skip("Need a region with at least 2 zones and instances")
		}

		zone1, zone2 := zones[0], zones[1]
		var tc2 *remote.TestContext
		for _, ctx := range testContexts {
			_, z, _ := ctx.Instance.GetIdentity()
			if z == zone2 && tc2 == nil {
				tc2 = ctx
			}
		}

		By(fmt.Sprintf("Creating a Regional PD in region %s (zones %s, %s)", selectedRegion, zone1, zone2))
		volName := testNamePrefix + string(uuid.NewUUID())
		topReq := &csi.TopologyRequirement{
			Requisite: []*csi.Topology{
				{Segments: map[string]string{constants.TopologyKeyZone: zone1}},
				{Segments: map[string]string{constants.TopologyKeyZone: zone2}},
			},
		}
		resp, err := tc.Client.CreateVolume(volName, map[string]string{
			parameters.ParameterKeyReplicationType: "regional-pd",
		}, defaultRepdSizeGb, topReq, nil)
		Expect(err).To(BeNil(), "CreateVolume failed for RePD")
		volID := resp.GetVolumeId()
		defer func() {
			if volID != "" {
				_ = tc.Client.DeleteVolume(volID)
			}
		}()

		By(fmt.Sprintf("Provisioning a temporary instance in zone %s to simulate failure", zone1))
		tempNodeName := fmt.Sprintf("%s-repd-fail-%s", testNamePrefix, string(uuid.NewUUID())[:8])
		tempInst, err := remote.SetupInstance(remote.InstanceConfig{
			Project:                   p,
			Architecture:              *architecture,
			MinCpuPlatform:            *minCpuPlatform,
			Zone:                      zone1,
			Name:                      tempNodeName,
			MachineType:               *machineType,
			ServiceAccount:            *serviceAccount,
			ImageURL:                  *imageURL,
			CloudtopHost:              *cloudtopHost,
			EnableConfidentialCompute: *enableConfidentialCompute,
			ComputeService:            computeService,
		})
		Expect(err).To(BeNil(), "Failed to setup temp instance in zone1")
		tempTC, err := testutils.GCEClientAndDriverSetup(tempInst, getDriverConfig())
		Expect(err).To(BeNil(), "Failed to setup CSI driver on temp instance")

		By("Attaching Regional PD to instance in Zone 1")
		err = tempTC.Client.ControllerPublishVolumeReadWrite(volID, tempInst.GetNodeID(), false /* forceAttach */)
		Expect(err).To(BeNil(), "Failed to attach RePD to zone 1 node")

		By("Deleting the instance in Zone 1 (Unclean detach simulating zone/node failure)")
		tempInst.DeleteInstance()

		By(fmt.Sprintf("Attaching Regional PD to instance in Zone 2 (%s) after failure", zone2))
		Eventually(func() error {
			return tc2.Client.ControllerPublishVolumeReadWrite(volID, tc2.Instance.GetNodeID(), false)
		}, 3*time.Minute, 10*time.Second).Should(BeNil(),"Failed to failover RePD to zone 2 after zone 1 node was deleted")

		By("Cleaning up the attachment")
		err = tc2.Client.ControllerUnpublishVolume(volID, tc2.Instance.GetNodeID())
		Expect(err).To(BeNil(), "ControllerUnpublishVolume failed")
	})

	It("Should handle volume recovery after simulated Spot/Pre-emptible node termination", func() {
		// This test simulates a Spot instance being reclaimed, driver must
		// handle the abrupt disappearance and allow the volume to be attached elsewhere.

		By("Creating a volume")
		volName := testNamePrefix + string(uuid.NewUUID())
		resp, err := tc.Client.CreateVolume(volName, nil, defaultSizeGb, nil, nil)
		Expect(err).To(BeNil(), "CreateVolume failed")
		volID := resp.GetVolumeId()
		defer func() {
			if volID != "" {
				_ = tc.Client.DeleteVolume(volID)
			}
		}()

		By("Provisioning a temporary 'Spot' instance")
		tempNodeName := fmt.Sprintf("%s-spot-%s", testNamePrefix, string(uuid.NewUUID())[:8])
		instanceConfig := remote.InstanceConfig{
			Project:                   p,
			Architecture:              *architecture,
			MinCpuPlatform:            *minCpuPlatform,
			Zone:                      z,
			Name:                      tempNodeName,
			MachineType:               *machineType,
			ServiceAccount:            *serviceAccount,
			ImageURL:                  *imageURL,
			CloudtopHost:              *cloudtopHost,
			EnableConfidentialCompute: *enableConfidentialCompute,
			ComputeService:            computeService,
		}
		tempInstance, err := remote.SetupInstance(instanceConfig)
		Expect(err).To(BeNil(), "Failed to setup temp spot instance")

		tempTC, err := testutils.GCEClientAndDriverSetup(tempInstance, getDriverConfig())
		Expect(err).To(BeNil(), "Failed to setup CSI driver on temp instance")

		By("Attaching and mounting the volume on the spot node")
		err = tempTC.Client.ControllerPublishVolumeReadWrite(volID, tempInstance.GetNodeID(), false)
		Expect(err).To(BeNil())

		By("Simulating sudden Spot termination (deleting instance without unmounting)")
		tempInstance.DeleteInstance()

		By("Verifying the volume can be attached to a new node promptly")
		Eventually(func() error {
			return tc.Client.ControllerPublishVolumeReadWrite(volID, nodeID, false)
		}, 3*time.Minute, 10*time.Second).Should(BeNil(), "Failed to recover volume from terminated spot instance")

		By("Cleaning up")
		err = tc.Client.ControllerUnpublishVolume(volID, nodeID)
		Expect(err).To(BeNil())
	})

	It("Should handle StatefulSet-like scale-up with multiple volumes across multiple nodes", func() {
		// This test simulates a StatefulSet scaling up, where multiple volumes are
		// created and attached to different nodes in a specific order or concurrently.

		numNodes := len(testContexts)
		if numNodes < 2 {
			Skip("Need at least two nodes for StatefulSet scale-up test")
		}

		numVolumes := numNodes * 2
		volIDs := make([]string, numVolumes)

		By(fmt.Sprintf("Scaling up: Creating and attaching %d volumes across %d nodes", numVolumes, numNodes))
		var wg sync.WaitGroup
		errChan := make(chan error, numVolumes*2)

		for i := 0; i < numVolumes; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				defer GinkgoRecover()

				vName := fmt.Sprintf("%s-ss-%d-%s", testNamePrefix, idx, string(uuid.NewUUID())[:8])
				targetTC := testContexts[idx%numNodes]

				resp, err := targetTC.Client.CreateVolume(vName, nil, defaultSizeGb, nil, nil)
				if err != nil {
					errChan <- fmt.Errorf("CreateVolume %d failed: %v", idx, err)
					return
				}
				vID := resp.GetVolumeId()
				volIDs[idx] = vID

				err = targetTC.Client.ControllerPublishVolumeReadWrite(vID, targetTC.Instance.GetNodeID(), false)
				if err != nil {
					errChan <- fmt.Errorf("Attach %d failed: %v", idx, err)
					return
				}
			}(i)
		}
		wg.Wait()
		close(errChan)

		for err := range errChan {
			Expect(err).To(BeNil())
		}

		defer func() {
			for _, vid := range volIDs {
				if vid != "" {
					_ = tc.Client.DeleteVolume(vid)
				}
			}
		}()

		By("Scaling down: Detaching all volumes concurrently")
		for i := 0; i < numVolumes; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				defer GinkgoRecover()
				targetTC := testContexts[idx%numNodes]
				_ = targetTC.Client.ControllerUnpublishVolume(volIDs[idx], targetTC.Instance.GetNodeID())
			}(i)
		}
		wg.Wait()
	})

	It("Should handle volume expansion while the pod is migrating between nodes", func() {
		// This test simulates a scenario where a volume is expanded (e.g., due to
		// quota increase) exactly when the pod is moving between nodes.

		if len(testContexts) < 2 {
			Skip("Need at least two nodes for migration test")
		}

		node1 := testContexts[0]
		node2 := testContexts[1]

		By("Creating a volume")
		volName := testNamePrefix + string(uuid.NewUUID())
		resp, err := node1.Client.CreateVolume(volName, nil, 10, nil, nil)
		Expect(err).To(BeNil())
		volID := resp.GetVolumeId()
		defer func() {
			if volID != "" {
				_ = node1.Client.DeleteVolume(volID)
			}
		}()

		By("Attaching to Node 1")
		err = node1.Client.ControllerPublishVolumeReadWrite(volID, node1.Instance.GetNodeID(), false)
		Expect(err).To(BeNil())

		By("Starting detachment from Node 1 and Expansion concurrently")
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			defer GinkgoRecover()
			_ = node1.Client.ControllerUnpublishVolume(volID, node1.Instance.GetNodeID())
		}()
		go func() {
			defer wg.Done()
			defer GinkgoRecover()
			_ = node1.Client.ControllerExpandVolume(volID, 20)
		}()
		wg.Wait()

		By("Attaching to Node 2 and verifying size")
		err = node2.Client.ControllerPublishVolumeReadWrite(volID, node2.Instance.GetNodeID(), false)
		Expect(err).To(BeNil(), "Failed to attach to Node 2 after concurrent expand/detach")

		// Verify size
		p, key, err := common.VolumeIDToKey(volID)
		Expect(err).To(BeNil())
		cloudDisk, err := computeService.Disks.Get(p, key.Zone, key.Name).Do()
		Expect(err).To(BeNil())
		Expect(cloudDisk.SizeGb).To(BeNumerically(">=", 20))

		By("Cleaning up")
		err = node2.Client.ControllerUnpublishVolume(volID, node2.Instance.GetNodeID())
		Expect(err).To(BeNil())
	})

	It("Should handle high-density volume attachments on a single node (Scalability Stress)", func() {
		// This test pushes the driver by attaching many volumes to a single node
		// concurrently, simulating a very dense node in a scaled-up cluster.

		numVolumes := 15 // GCE has limits, 15 is safe but stressful for concurrent calls
		volIDs := make([]string, numVolumes)
		var wg sync.WaitGroup
		errChan := make(chan error, numVolumes)

		By(fmt.Sprintf("Attaching %d volumes to node %s concurrently", numVolumes, nodeID))
		for i := 0; i < numVolumes; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				defer GinkgoRecover()

				vName := fmt.Sprintf("%s-dense-%d-%s", testNamePrefix, idx, string(uuid.NewUUID())[:8])
				resp, err := tc.Client.CreateVolume(vName, nil, defaultSizeGb, nil, nil)
				if err != nil {
					errChan <- err
					return
				}
				vID := resp.GetVolumeId()
				volIDs[idx] = vID

				err = tc.Client.ControllerPublishVolumeReadWrite(vID, nodeID, false)
				if err != nil {
					errChan <- err
				}
			}(i)
		}
		wg.Wait()
		close(errChan)

		for err := range errChan {
			Expect(err).To(BeNil(), "Dense attachment failed")
		}

		defer func() {
			for _, vid := range volIDs {
				if vid != "" {
					_ = tc.Client.DeleteVolume(vid)
				}
			}
		}()

		By("Detaching all volumes concurrently")
		for i := 0; i < numVolumes; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				defer GinkgoRecover()
				_ = tc.Client.ControllerUnpublishVolume(volIDs[idx], nodeID)
			}(i)
		}
		wg.Wait()
	})

	It("Should handle rapid attachment and detachment of Regional PDs across multiple zones", func() {
		// This test simulates a Regional PD being rapidly moved between nodes in different zones
		// due to pods being rescheduled by the autoscaler.

		By("Identifying two zones with instances")
		if len(testContexts) < 2 {
			Skip("Need at least two nodes in different zones")
		}

		tc1 := testContexts[0]
		tc2 := testContexts[1]
		_, z1, _ := tc1.Instance.GetIdentity()
		_, z2, _ := tc2.Instance.GetIdentity()

		if z1 == z2 {
			Skip("Need instances in different zones for RePD migration test")
		}

		reg, _ := common.GetRegionFromZones([]string{z1})
		volName := testNamePrefix + string(uuid.NewUUID())
		topReq := &csi.TopologyRequirement{
			Requisite: []*csi.Topology{
				{Segments: map[string]string{constants.TopologyKeyZone: z1}},
				{Segments: map[string]string{constants.TopologyKeyZone: z2}},
			},
		}

		By(fmt.Sprintf("Creating Regional PD in region %s", reg))
		resp, err := tc1.Client.CreateVolume(volName, map[string]string{
			parameters.ParameterKeyReplicationType: "regional-pd",
		}, defaultRepdSizeGb, topReq, nil)
		Expect(err).To(BeNil())
		volID := resp.GetVolumeId()
		defer func() {
			if volID != "" {
				_ = tc1.Client.DeleteVolume(volID)
			}
		}()

		By("Rapidly migrating RePD between Zone 1 and Zone 2 multiple times")
		for i := 0; i < 3; i++ {
			By(fmt.Sprintf("Migration Iteration %d: Zone 1 -> Zone 2", i))

			err = tc1.Client.ControllerPublishVolumeReadWrite(volID, tc1.Instance.GetNodeID(), false)
			Expect(err).To(BeNil())
			err = tc1.Client.ControllerUnpublishVolume(volID, tc1.Instance.GetNodeID())
			Expect(err).To(BeNil())

			err = tc2.Client.ControllerPublishVolumeReadWrite(volID, tc2.Instance.GetNodeID(), false)
			Expect(err).To(BeNil())
			err = tc2.Client.ControllerUnpublishVolume(volID, tc2.Instance.GetNodeID())
			Expect(err).To(BeNil())
		}
	})

	It("Should recover volume after node pool scale-down event", func() {
		node1, err := createAutoscalingTempNodeContext(p, z, "scaledown-src")
		Expect(err).To(BeNil())
		//defer cleanupAutoscalingTempNodeContext(node1)

		By("Creating volume and attaching to node1")
		volName := testNamePrefix + string(uuid.NewUUID())
		resp, err := tc.Client.CreateVolume(volName, nil, defaultSizeGb, nil, nil)
		Expect(err).To(BeNil())
		volID := resp.GetVolumeId()
		defer func() {
			if volID != "" {
				_ = tc.Client.DeleteVolume(volID)
			}
		}()

		err = node1.Client.ControllerPublishVolumeReadWrite(volID, node1.Instance.GetNodeID(), false)
		Expect(err).To(BeNil())

		By("Simulating autoscaler scale-down (deleting node1)")
		node1.Instance.DeleteInstance()
		Expect(waitForInstanceNotFound(p, z, node1.Instance.GetName())).To(BeNil())
		Expect(waitForDiskUsersToDrain(p, z, volName)).To(BeNil())

		By("Verifying volume re-attachment to a healthy node")
		Eventually(func() error {
			return tc.Client.ControllerPublishVolumeReadWrite(volID, nodeID, false)
		}, 4*time.Minute, 15*time.Second).Should(BeNil())

		_ = tc.Client.ControllerUnpublishVolume(volID, nodeID)
	})

	It("Should provision multiple nodes during rapid scale-up within acceptable timing", func() {
		numScaleUps := 3
		var wg sync.WaitGroup
		errChan := make(chan error, numScaleUps)
		createdInstances := make(chan *remote.InstanceInfo, numScaleUps)

		start := time.Now()

		for i := 0; i < numScaleUps; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				defer GinkgoRecover()

				name := fmt.Sprintf("%sburst-%d-%s", testNamePrefix, idx, string(uuid.NewUUID())[:6])

				cfg := remote.InstanceConfig{
					Project:                   p,
					Architecture:              *architecture,
					MinCpuPlatform:            *minCpuPlatform,
					Zone:                      z,
					Name:                      name,
					MachineType:               *machineType,
					ServiceAccount:            *serviceAccount,
					ImageURL:                  *imageURL,
					CloudtopHost:              *cloudtopHost,
					EnableConfidentialCompute: *enableConfidentialCompute,
					ComputeService:            computeService,
				}

				instance, err := remote.SetupInstance(cfg)
				if err != nil {
					errChan <- err
					return
				}
				createdInstances <- instance

			}(i)
		}

		wg.Wait()
		close(errChan)

		for err := range errChan {
			Expect(err).To(BeNil())
		}
		close(createdInstances)
		for inst := range createdInstances {
			inst.DeleteInstance()
		}

		totalDuration := time.Since(start)
		By(fmt.Sprintf("Burst scale-up completed in %v", totalDuration))	
		Expect(totalDuration).To(BeNumerically("<", 10*time.Minute))
	})

	It("Should reschedule volume correctly after autoscaler-driven rescheduling", func() {
		baseNode, err := createAutoscalingTempNodeContext(p, z, "resched-src")
		Expect(err).To(BeNil())
		defer cleanupAutoscalingTempNodeContext(baseNode)

		By("Creating and attaching volume to base node")
		volName := testNamePrefix + string(uuid.NewUUID())
		resp, err := tc.Client.CreateVolume(volName, nil, defaultSizeGb, nil, nil)
		Expect(err).To(BeNil())
		volID := resp.GetVolumeId()
		defer func() {
			_ = tc.Client.DeleteVolume(volID)
		}()

		err = baseNode.Client.ControllerPublishVolumeReadWrite(volID, baseNode.Instance.GetNodeID(), false)
		Expect(err).To(BeNil())

		By("Simulating autoscaler replacing node")
		baseNode.Instance.DeleteInstance()
		Expect(waitForInstanceNotFound(p, z, baseNode.Instance.GetName())).To(BeNil())
		Expect(waitForDiskUsersToDrain(p, z, volName)).To(BeNil())

		newTC, err := createAutoscalingTempNodeContext(p, z, "resched-dst")
		Expect(err).To(BeNil())
		defer cleanupAutoscalingTempNodeContext(newTC)

		By("Verifying volume attaches to replacement node")
		Eventually(func() error {
			return newTC.Client.ControllerPublishVolumeReadWrite(volID, newTC.Instance.GetNodeID(), false)
		}, 4*time.Minute, 15*time.Second).Should(BeNil())

		_ = newTC.Client.ControllerUnpublishVolume(volID, newTC.Instance.GetNodeID())
	})

})

func createAutoscalingTempNodeContext(project, zone, suffix string) (*remote.TestContext, error) {
	nodeName := fmt.Sprintf("%s%s-%s", testNamePrefix, suffix, string(uuid.NewUUID())[:8])
	cfg := remote.InstanceConfig{
		Project:                   project,
		Architecture:              *architecture,
		MinCpuPlatform:            *minCpuPlatform,
		Zone:                      zone,
		Name:                      nodeName,
		MachineType:               *machineType,
		ServiceAccount:            *serviceAccount,
		ImageURL:                  *imageURL,
		CloudtopHost:              *cloudtopHost,
		EnableConfidentialCompute: *enableConfidentialCompute,
		ComputeService:            computeService,
		LocalSSDCount:             0,
	}
	instance, err := remote.SetupInstance(cfg)
	if err != nil {
		return nil, err
	}

	if err := testutils.MkdirAll(instance, "/lib/udev_containerized"); err != nil {
		instance.DeleteInstance()
		return nil, err
	}
	if err := testutils.CopyFile(instance, "/lib/udev/scsi_id", "/lib/udev_containerized/scsi_id"); err != nil {
		instance.DeleteInstance()
		return nil, err
	}
	if err := testutils.CopyFile(instance, "/lib/udev/google_nvme_id", "/lib/udev_containerized/google_nvme_id"); err != nil {
		instance.DeleteInstance()
		return nil, err
	}
	if err := testutils.InstallDependencies(instance, []string{"lvm2", "mdadm", "grep", "coreutils"}); err != nil {
		instance.DeleteInstance()
		return nil, err
	}
	if err := testutils.SetupDataCachingConfig(instance); err != nil {
		instance.DeleteInstance()
		return nil, err
	}

	newTC, err := testutils.GCEClientAndDriverSetup(instance, getDriverConfig())
	if err != nil {
		instance.DeleteInstance()
		return nil, err
	}
	return newTC, nil
}

func cleanupAutoscalingTempNodeContext(tc *remote.TestContext) {
	if tc == nil {
		return
	}

	if err := remote.TeardownDriverAndClient(tc); err != nil {
		klog.Warningf("Teardown failed for temp autoscaling context %s: %v", tc.Instance.GetName(), err)
	}
	tc.Instance.DeleteInstance()
}

func waitForInstanceNotFound(project, zone, instanceName string) error {
	return wait.Poll(10*time.Second, 4*time.Minute, func() (bool, error) {
		_, err := computeService.Instances.Get(project, zone, instanceName).Do()
		if gce.IsGCEError(err, "notFound") {
			return true, nil
		}
		if err != nil {
			return false, nil
		}
		return false, nil
	})
}

func waitForDiskUsersToDrain(project, zone, volName string) error {
	return wait.Poll(10*time.Second, 4*time.Minute, func() (bool, error) {
		disk, err := computeService.Disks.Get(project, zone, volName).Do()
		if err != nil {
			return false, nil
		}
		return len(disk.Users) == 0, nil
	})
}
