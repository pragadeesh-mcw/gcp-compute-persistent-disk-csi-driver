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
	"sigs.k8s.io/gcp-compute-persistent-disk-csi-driver/pkg/common"
	"sigs.k8s.io/gcp-compute-persistent-disk-csi-driver/pkg/constants"
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
		err = tc2.Client.ControllerPublishVolumeReadWrite(volID, tc2.Instance.GetNodeID(), false /* forceAttach */)
		Expect(err).To(BeNil(), "Failed to failover RePD to zone 2 after zone 1 node was deleted")

		By("Cleaning up the attachment")
		err = tc2.Client.ControllerUnpublishVolume(volID, tc2.Instance.GetNodeID())
		Expect(err).To(BeNil(), "ControllerUnpublishVolume failed")
	})

	It("Should fail to attach a SINGLE_NODE_WRITER volume to a second node while already attached (Negative case)", func() {
		// This test ensures the driver enforces exclusive access for SINGLE_NODE_WRITER volumes,
		// which is critical for preventing data corruption during rapid scaling/pod migration.

		if len(testContexts) < 2 {
			Skip("Need at least two nodes for attachment conflict test")
		}

		node1 := testContexts[0]
		node2 := testContexts[1]

		By("Creating a SINGLE_NODE_WRITER volume")
		volName := testNamePrefix + string(uuid.NewUUID())
		resp, err := node1.Client.CreateVolume(volName, nil, defaultSizeGb, nil, nil)
		Expect(err).To(BeNil(), "CreateVolume failed")
		volID := resp.GetVolumeId()
		defer func() {
			if volID != "" {
				_ = node1.Client.DeleteVolume(volID)
			}
		}()

		By(fmt.Sprintf("Attaching volume to first node %s", node1.Instance.GetName()))
		err = node1.Client.ControllerPublishVolumeReadWrite(volID, node1.Instance.GetNodeID(), false /* forceAttach */)
		Expect(err).To(BeNil(), "First attachment failed")

		By(fmt.Sprintf("Attempting to attach the same volume to second node %s (expected failure)", node2.Instance.GetName()))
		// The GCE PD CSI driver should reject this because the disk is already attached to another node
		// and it's not a multi-writer disk.
		err = node2.Client.ControllerPublishVolumeReadWrite(volID, node2.Instance.GetNodeID(), false /* forceAttach */)
		Expect(err).ToNot(BeNil(), "Expected second attachment to fail, but it succeeded")
		Expect(err.Error()).To(
			Or(
				ContainSubstring("RESOURCE_IN_USE_BY_ANOTHER_RESOURCE"),
				ContainSubstring("already being used"),
				ContainSubstring("already attached"),
			),
			"Error message should indicate the disk is already attached",
		)

		By("Detaching from first node and verifying it can then be attached to second node")
		err = node1.Client.ControllerUnpublishVolume(volID, node1.Instance.GetNodeID())
		Expect(err).To(BeNil(), "First detachment failed")

		err = node2.Client.ControllerPublishVolumeReadWrite(volID, node2.Instance.GetNodeID(), false /* forceAttach */)
		Expect(err).To(BeNil(), "Second attachment failed after first node detached")

		By("Cleaning up")
		err = node2.Client.ControllerUnpublishVolume(volID, node2.Instance.GetNodeID())
		Expect(err).To(BeNil())
	})
})
