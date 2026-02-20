/*
Copyright 2018 The Kubernetes Authors.

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
	csi "github.com/container-storage-interface/spec/lib/go/csi"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"path/filepath"
	"strings"
	testutils "sigs.k8s.io/gcp-compute-persistent-disk-csi-driver/test/e2e/utils"
	"k8s.io/klog/v2"
	"k8s.io/apimachinery/pkg/util/uuid"
	"sigs.k8s.io/gcp-compute-persistent-disk-csi-driver/pkg/common"
	"sigs.k8s.io/gcp-compute-persistent-disk-csi-driver/pkg/constants"
	gce "sigs.k8s.io/gcp-compute-persistent-disk-csi-driver/pkg/gce-cloud-provider/compute"
	"sigs.k8s.io/gcp-compute-persistent-disk-csi-driver/pkg/parameters"
	remote "sigs.k8s.io/gcp-compute-persistent-disk-csi-driver/test/remote"
)

var _ = Describe("GCE PD CSI Driver Scheduling Tests", func() {
	var (
		volName   string
		volID     string
	)

	BeforeEach(func() {
		volName = testNamePrefix + string(uuid.NewUUID())
		volID = "" // Reset volID for each test
	})

	AfterEach(func() {
		if volID != "" {
			// We don't know for sure if it's zonal or regional in the generic AfterEach
			// so we try both or use common.VolumeIDToKey to decide.
			p, key, err := common.VolumeIDToKey(volID)
			if err == nil {
				By(fmt.Sprintf("Cleaning up volume %s (ID: %s)", volName, volID))
				// Using a random client is fine for deletion as long as it has permissions
				testContext := getRandomTestContext()
				_ = testContext.Client.DeleteVolume(volID)

				// Verify disk deletion from GCP
				if key.Region != "" {
					_, err = computeService.RegionDisks.Get(p, key.Region, key.Name).Do()
				} else {
					_, err = computeService.Disks.Get(p, key.Zone, key.Name).Do()
				}
				Expect(gce.IsGCEError(err, "notFound")).To(BeTrue(), "Expected disk %s to not be found. Got: %v", volName, err)
			}
		}
	})

	It("Should successfully provision and attach a zonal PD to a specific zone", func() {
		By("Selecting a test context and its zone")
		testContext := getRandomTestContext()
		controllerClient := testContext.Client
		instance := testContext.Instance
		_, zone, _ := instance.GetIdentity()

		By(fmt.Sprintf("Creating a volume %s in Zone %s (simulating topological provisioning)", volName, zone))
		topReq := &csi.TopologyRequirement{
			Requisite: []*csi.Topology{
				{Segments: map[string]string{constants.TopologyKeyZone: zone}},
			},
			Preferred: []*csi.Topology{
				{Segments: map[string]string{constants.TopologyKeyZone: zone}},
			},
		}

		resp, err := controllerClient.CreateVolume(volName, map[string]string{
			parameters.ParameterKeyType: "pd-standard",
		}, defaultSizeGb, topReq, nil)

		Expect(err).To(BeNil(), "Failed to create volume %s in zone %s: %v", volName, zone, err)
		volID = resp.GetVolumeId()
		Expect(volID).ToNot(BeEmpty(), "Volume ID was empty")

		By(fmt.Sprintf("Successfully attaching volume %s to instance %s in zone %s", volName, instance.GetName(), zone))
		err = testAttachWriteReadDetach(volID, volName, instance, controllerClient, false /* readOnly */, false /* detachAndReattach */, false /* setupDataCache */)
		Expect(err).To(BeNil(), "Failed to attach and verify volume in its designated zone")
	})

	It("Should fail to attach a zonal PD to an instance in a different zone", func() {
		By("Getting two instances from different zones")
		if len(testContexts) < 2 {
			Skip("Not enough test contexts in different zones to run this test.")
		}

		zoneMap := make(map[string]*remote.TestContext)
		for _, tc := range testContexts {
			_, z, _ := tc.Instance.GetIdentity()
			zoneMap[z] = tc
		}

		if len(zoneMap) < 2 {
			Skip("Could not find two instances in different zones.")
		}

		zonesFound := []string{}
		for z := range zoneMap {
			zonesFound = append(zonesFound, z)
		}

		zoneA := zonesFound[0]
		zoneB := zonesFound[1]
		testContextA := zoneMap[zoneA]
		testContextB := zoneMap[zoneB]
		instanceB := testContextB.Instance

		By(fmt.Sprintf("Creating a pd-standard volume %s in Zone %s", volName, zoneA))
		resp, err := testContextA.Client.CreateVolume(volName, map[string]string{
			parameters.ParameterKeyType: "pd-standard",
		}, defaultSizeGb, &csi.TopologyRequirement{
			Requisite: []*csi.Topology{
				{Segments: map[string]string{constants.TopologyKeyZone: zoneA}},
			},
		}, nil)

		Expect(err).To(BeNil(), "Failed to create volume %s in zone %s: %v", volName, zoneA, err)
		volID = resp.GetVolumeId()

		By(fmt.Sprintf("Attempting to attach volume %s (from Zone %s) to instance %s (in Zone %s)", volName, zoneA, instanceB.GetName(), zoneB))
		// Expect this to fail in ControllerPublishVolume because the driver should validate that the disks are in the same zone
		err = testContextB.Client.ControllerPublishVolumeReadWrite(volID, instanceB.GetNodeID(), false /* forceAttach */)
		Expect(err).ToNot(BeNil(), "Expected volume attachment to fail across zones, but it succeeded.")
		Expect(err.Error()).To(ContainSubstring("Disk must be in the same zone"), "Error message did not contain 'Disk must be in the same zone'.")
	})

	It("Should successfully failover a regional PD between instances in its replica zones", func() {
		By("Identifying two zones in the same region with instances")
		regionMap := make(map[string]map[string]*remote.TestContext)
		for _, tc := range testContexts {
			_, z, _ := tc.Instance.GetIdentity()
			region, err := common.GetRegionFromZones([]string{z})
			Expect(err).To(BeNil())
			if _, ok := regionMap[region]; !ok {
				regionMap[region] = make(map[string]*remote.TestContext)
			}
			regionMap[region][z] = tc
		}

		var selectedRegion string
		var zone1, zone2 string
		for r, zonesInRegion := range regionMap {
			if len(zonesInRegion) >= 2 {
				selectedRegion = r
				zList := []string{}
				for z := range zonesInRegion {
					zList = append(zList, z)
				}
				zone1 = zList[0]
				zone2 = zList[1]
				break
			}
		}

		if selectedRegion == "" {
			Skip("Could not find a region with instances in at least two zones.")
		}

		testContext1 := regionMap[selectedRegion][zone1]
		testContext2 := regionMap[selectedRegion][zone2]

		By(fmt.Sprintf("Creating a regional PD %s in region %s (zones %s, %s)", volName, selectedRegion, zone1, zone2))
		resp, err := testContext1.Client.CreateVolume(volName, map[string]string{
			parameters.ParameterKeyReplicationType: "regional-pd",
		}, defaultRepdSizeGb, &csi.TopologyRequirement{
			Requisite: []*csi.Topology{
				{Segments: map[string]string{constants.TopologyKeyZone: zone1}},
				{Segments: map[string]string{constants.TopologyKeyZone: zone2}},
			},
		}, nil)
		Expect(err).To(BeNil())
		volID = resp.GetVolumeId()

		By(fmt.Sprintf("Attaching to instance in %s and writing data", zone1))
		writeFile, _ := testWriteAndReadFile(testContext1.Instance, false /* readOnly */)
		err = testLifecycleWithVerify(volID, volName, testContext1.Instance, testContext1.Client, false /* readOnly */, false /* fs */, writeFile, nil /* verifyReadFile */, false /* detachAndReattach */, false /* setupDataCache */)
		Expect(err).To(BeNil(), "Failed initial attachment and write in zone1")

		By(fmt.Sprintf("Attaching to instance in %s and reading data (failover)", zone2))
		noOpWrite := func(a *verifyArgs) error { return nil }
		_, verifyReadFile := testWriteAndReadFile(testContext2.Instance, false /* readOnly */)
		err = testLifecycleWithVerify(volID, volName, testContext2.Instance, testContext2.Client, false /* readOnly */, false /* fs */, noOpWrite, verifyReadFile, false /* detachAndReattach */, false /* setupDataCache */)
		Expect(err).To(BeNil(), "Failed failover attachment and read in zone2")
	})

	It("Should fail to provision a volume when requisite topology cannot be satisfied", func() {
		By("Requesting a volume in a non-existent zone")
		invalidZone := "us-north1-a"
		testContext := getRandomTestContext()

		topReq := &csi.TopologyRequirement{
			Requisite: []*csi.Topology{
				{Segments: map[string]string{constants.TopologyKeyZone: invalidZone}},
			},
		}

		_, err := testContext.Client.CreateVolume(volName, map[string]string{
			parameters.ParameterKeyType: "pd-standard",
		}, defaultSizeGb, topReq, nil)

		Expect(err).ToNot(BeNil(), "Expected CreateVolume to fail for non-existent zone")
	})

	It("Should respect preferred topology when requisite topology allows multiple zones", func() {
		By("Identifying two zones in the same region")
		zoneMap := make(map[string]*remote.TestContext)
		for _, tc := range testContexts {
			_, z, _ := tc.Instance.GetIdentity()
			zoneMap[z] = tc
		}
		
		if len(zoneMap) < 2 {
			Skip("Need at least two zones.")
		}
		
		zonesFound := []string{}
		for z := range zoneMap {
			zonesFound = append(zonesFound, z)
		}
		zoneA := zonesFound[0]
		zoneB := zonesFound[1]

		By(fmt.Sprintf("Creating a zonal PD with requisite zones {%s, %s} and preferred zone %s", zoneA, zoneB, zoneB))
		testContext := zoneMap[zoneB]
		topReq := &csi.TopologyRequirement{
			Requisite: []*csi.Topology{
				{Segments: map[string]string{constants.TopologyKeyZone: zoneA}},
				{Segments: map[string]string{constants.TopologyKeyZone: zoneB}},
			},
			Preferred: []*csi.Topology{
				{Segments: map[string]string{constants.TopologyKeyZone: zoneB}},
			},
		}

		resp, err := testContext.Client.CreateVolume(volName, map[string]string{
			parameters.ParameterKeyType: "pd-standard",
		}, defaultSizeGb, topReq, nil)
		Expect(err).To(BeNil())
		volID = resp.GetVolumeId()

		By("Verifying the volume was created in the preferred zone")
		_, key, err := common.VolumeIDToKey(volID)
		Expect(err).To(BeNil())
		Expect(key.Zone).To(Equal(zoneB), "Volume should have been created in preferred zone %s but was in %s", zoneB, key.Zone)
	})

	It("Should successfully provision a regional PD with preferred topology being a subset of requisite", func() {
		By("Identifying two zones in the same region")
		regionMap := make(map[string][]string)
		for _, tc := range testContexts {
			_, z, _ := tc.Instance.GetIdentity()
			region, _ := common.GetRegionFromZones([]string{z})
			regionMap[region] = append(regionMap[region], z)
		}

		var selectedRegion string
		var zonesInRegion []string
		for r, zs := range regionMap {
			if len(zs) >= 2 {
				selectedRegion = r
				zonesInRegion = zs
				break
			}
		}

		if selectedRegion == "" {
			Skip("Could not find a region with at least two zones.")
		}

		zone1 := zonesInRegion[0]
		zone2 := zonesInRegion[1]
		testContext := getRandomTestContext()

		By(fmt.Sprintf("Creating regional PD with requisite zones {%s, %s} and preferred zone %s", zone1, zone2, zone1))
		topReq := &csi.TopologyRequirement{
			Requisite: []*csi.Topology{
				{Segments: map[string]string{constants.TopologyKeyZone: zone1}},
				{Segments: map[string]string{constants.TopologyKeyZone: zone2}},
			},
			Preferred: []*csi.Topology{
				{Segments: map[string]string{constants.TopologyKeyZone: zone1}},
			},
		}

		resp, err := testContext.Client.CreateVolume(volName, map[string]string{
			parameters.ParameterKeyReplicationType: "regional-pd",
		}, defaultRepdSizeGb, topReq, nil)
		Expect(err).To(BeNil())
		volID = resp.GetVolumeId()

		By("Verifying the regional PD has the correct replica zones")
		p, key, err := common.VolumeIDToKey(volID)
		Expect(err).To(BeNil())
		Expect(key.Region).To(Equal(selectedRegion))

		cloudDisk, err := computeService.RegionDisks.Get(p, key.Region, key.Name).Do()
		Expect(err).To(BeNil())
		
		replicaZones := []string{}
		for _, rz := range cloudDisk.ReplicaZones {
			replicaZones = append(replicaZones, zoneFromURL(rz))
		}
		Expect(replicaZones).To(ConsistOf(zone1, zone2))
	})

	It("Should successfully restore a zonal PD from a snapshot in a different zone", func() {
		By("Identifying two instances in different zones")

		if len(testContexts) < 2 {
			Skip("Not enough test contexts.")
		}

		tc1 := testContexts[0]
		tc2 := testContexts[1]

		_, zone1, _ := tc1.Instance.GetIdentity()
		_, zone2, _ := tc2.Instance.GetIdentity()

		if zone1 == zone2 {
			Skip("Need two instances in different zones.")
		}

		By(fmt.Sprintf("Creating source volume in zone %s and writing data", zone1))

		resp, err := tc1.Client.CreateVolume(volName, map[string]string{
			parameters.ParameterKeyType: "pd-standard",
		}, defaultSizeGb, &csi.TopologyRequirement{
			Requisite: []*csi.Topology{
				{Segments: map[string]string{constants.TopologyKeyZone: zone1}},
			},
		}, nil)
		Expect(err).To(BeNil())

		srcVolID := resp.GetVolumeId()
		defer tc1.Client.DeleteVolume(srcVolID)

		// Attach source volume
		err = tc1.Client.ControllerPublishVolumeReadWrite(srcVolID, tc1.Instance.GetNodeID(), false)
		Expect(err).To(BeNil())

		// Unique mount dirs for SOURCE volume
		srcStageDir := filepath.Join("/tmp", volName, "src-stage")
		srcMountDir := filepath.Join("/tmp", volName, "src-mount")

		// Stage + Publish source volume
		err = tc1.Client.NodeStageExt4Volume(srcVolID, srcStageDir, false)
		Expect(err).To(BeNil())

		err = tc1.Client.NodePublishVolume(srcVolID, srcStageDir, srcMountDir)
		Expect(err).To(BeNil())

		By("Verifying source volume is actually mounted")

		mountOut, _ := tc1.Instance.SSH(fmt.Sprintf("mount | grep %s", srcMountDir))
		//fmt.Fprintf(GinkgoWriter, "SOURCE mount output:\n%s\n", mountOut)

		Expect(mountOut).NotTo(BeEmpty(), "Source volume was not mounted correctly")

		By("Writing test content to source volume")

		testContent := "Snapshot cross-zone data"
		testFile := filepath.Join(srcMountDir, "snap-test")

		_, err = tc1.Instance.SSH(fmt.Sprintf(
			"echo '%s' | sudo tee %s > /dev/null && sync",
			testContent,
			testFile,
		))
		Expect(err).To(BeNil())

		// Confirm file exists
		lsOut, _ := tc1.Instance.SSH("sudo cat " + testFile)
		//fmt.Fprintf(GinkgoWriter, "SOURCE file content:\n%s\n", lsOut)

		Expect(strings.TrimSpace(lsOut)).To(Equal(testContent))

		By("Unmounting source volume before snapshot")

		err = tc1.Client.NodeUnpublishVolume(srcVolID, srcMountDir)
		Expect(err).To(BeNil())

		err = tc1.Client.NodeUnstageVolume(srcVolID, srcStageDir)
		Expect(err).To(BeNil())

		err = tc1.Client.ControllerUnpublishVolume(srcVolID, tc1.Instance.GetNodeID())
		Expect(err).To(BeNil())

		By("Creating snapshot of source volume")

		snapName := "snap-" + string(uuid.NewUUID())

		snapID, err := tc1.Client.CreateSnapshot(snapName, srcVolID, nil)
		Expect(err).To(BeNil())
		defer tc1.Client.DeleteSnapshot(snapID)

		By("Waiting for snapshot to become READY")

		Eventually(func() (string, error) {
			p, _, _, err := common.SnapshotIDToProjectKey(snapID)
			if err != nil {
				return "", err
			}

			snap, err := computeService.Snapshots.Get(p, snapName).Do()
			if err != nil {
				return "", err
			}
			return snap.Status, nil
		}, "5m", "10s").Should(Equal("READY"))

		By(fmt.Sprintf("Restoring snapshot into a new volume in zone %s", zone2))

		restoreVolName := "restore-" + string(uuid.NewUUID())

		resp, err = tc2.Client.CreateVolume(restoreVolName, map[string]string{
			parameters.ParameterKeyType: "pd-standard",
		}, defaultSizeGb, &csi.TopologyRequirement{
			Requisite: []*csi.Topology{
				{Segments: map[string]string{constants.TopologyKeyZone: zone2}},
			},
		}, &csi.VolumeContentSource{
			Type: &csi.VolumeContentSource_Snapshot{
				Snapshot: &csi.VolumeContentSource_SnapshotSource{
					SnapshotId: snapID,
				},
			},
		})
		Expect(err).To(BeNil())

		restoredVolID := resp.GetVolumeId()
		defer tc2.Client.DeleteVolume(restoredVolID)

		// Attach restored volume
		err = tc2.Client.ControllerPublishVolumeReadWrite(restoredVolID, tc2.Instance.GetNodeID(), false)
		Expect(err).To(BeNil())

		// Unique mount dirs for RESTORE volume
		restoreStageDir := filepath.Join("/tmp", restoreVolName, "restore-stage")
		restoreMountDir := filepath.Join("/tmp", restoreVolName, "restore-mount")

		// Stage + Publish restored volume
		err = tc2.Client.NodeStageExt4Volume(restoredVolID, restoreStageDir, false)
		Expect(err).To(BeNil())

		err = tc2.Client.NodePublishVolume(restoredVolID, restoreStageDir, restoreMountDir)
		Expect(err).To(BeNil())

		By("Verifying restored volume is actually mounted")

		restoreMountOut, _ := tc2.Instance.SSH(fmt.Sprintf("mount | grep %s", restoreMountDir))
		//fmt.Fprintf(GinkgoWriter, "RESTORE mount output:\n%s\n", restoreMountOut)

		Expect(restoreMountOut).NotTo(BeEmpty(), "Restored volume was not mounted correctly")

		By("Reading restored file content")

		restoredFile := filepath.Join(restoreMountDir, "snap-test")

		content, err := testutils.ReadFileWithSudo(tc2.Instance, restoredFile)
		Expect(err).To(BeNil())

		//fmt.Fprintf(GinkgoWriter, "RESTORED file content: %q\n", string(content))

		Expect(strings.TrimSpace(string(content))).To(Equal(testContent))

		By("Cleaning up restored volume")

		_ = tc2.Client.NodeUnpublishVolume(restoredVolID, restoreMountDir)
		_ = tc2.Client.NodeUnstageVolume(restoredVolID, restoreStageDir)
		_ = tc2.Client.ControllerUnpublishVolume(restoredVolID, tc2.Instance.GetNodeID())
	})

	It("Should fail to attach a regional PD to an instance in a different region", func() {
		By("Getting a regional PD and an out-of-region instance")
		if len(testContexts) < 2 {
			Skip("Not enough test contexts to run this test.")
		}

		regionMap := make(map[string]map[string]*remote.TestContext)
		for _, tc := range testContexts {
			_, z, _ := tc.Instance.GetIdentity()
			region, err := common.GetRegionFromZones([]string{z})
			Expect(err).To(BeNil())
			if _, ok := regionMap[region]; !ok {
				regionMap[region] = make(map[string]*remote.TestContext)
			}
			regionMap[region][z] = tc
		}

		if len(regionMap) < 2 {
			Skip("Could not find instances in at least two different regions.")
		}

		regions := []string{}
		for r := range regionMap {
			regions = append(regions, r)
		}

		regionA := regions[0]
		regionB := regions[1]

		// Get two zones from regionA for RePD
		zListA := []string{}
		for z := range regionMap[regionA] {
			zListA = append(zListA, z)
		}
		if len(zListA) < 2 {
			// Try to find another region with 2 zones
			found := false
			for _, r := range regions {
				if len(regionMap[r]) >= 2 {
					regionA = r
					regionB = ""
					for _, rb := range regions {
						if rb != regionA {
							regionB = rb
							break
						}
					}
					zListA = []string{}
					for z := range regionMap[regionA] {
						zListA = append(zListA, z)
					}
					found = true
					break
				}
			}
			if !found {
				Skip("Could not find a region with at least two zones for RePD.")
			}
		}

		zoneA1 := zListA[0]
		zoneA2 := zListA[1]

		// Instance in regionB
		var instanceB *remote.InstanceInfo
		var clientB *remote.CsiClient
		for _, tc := range regionMap[regionB] {
			instanceB = tc.Instance
			clientB = tc.Client
			break
		}

		By(fmt.Sprintf("Creating a regional PD %s in region %s (zones %s, %s)", volName, regionA, zoneA1, zoneA2))
		resp, err := regionMap[regionA][zoneA1].Client.CreateVolume(volName, map[string]string{
			parameters.ParameterKeyReplicationType: "regional-pd",
		}, defaultRepdSizeGb, &csi.TopologyRequirement{
			Requisite: []*csi.Topology{
				{Segments: map[string]string{constants.TopologyKeyZone: zoneA1}},
				{Segments: map[string]string{constants.TopologyKeyZone: zoneA2}},
			},
		}, nil)
		Expect(err).To(BeNil())
		volID = resp.GetVolumeId()

		By(fmt.Sprintf("Attempting to attach regional PD %s to instance %s in a different region %s", volName, instanceB.GetName(), regionB))
		err = clientB.ControllerPublishVolumeReadWrite(volID, instanceB.GetNodeID(), false /* forceAttach */)
		Expect(err).ToNot(BeNil(), "Expected regional PD attachment to fail across regions")
		Expect(err.Error()).To(ContainSubstring("must have a replica"), "Error message did not contain 'must have a replica'.")
	})

	It("Should successfully attach a zonal PD to multiple nodes in the same zone as Read-Only (ROX)", func() {
		By("Identifying two instances in the same zone")

		zoneMap := make(map[string][]*remote.TestContext)
		for _, tc := range testContexts {
			_, z, _ := tc.Instance.GetIdentity()
			zoneMap[z] = append(zoneMap[z], tc)
		}

		var zone string
		var contexts []*remote.TestContext
		for z, tcs := range zoneMap {
			if len(tcs) >= 2 {
				zone = z
				contexts = tcs
				break
			}
		}
		if len(contexts) < 2 {
			Skip("Need at least two instances in the same zone.")
		}

		tc1 := contexts[0]
		tc2 := contexts[1]
		inst1 := tc1.Instance
		inst2 := tc2.Instance
		client1 := tc1.Client
		client2 := tc2.Client

		volName := testNamePrefix + string(uuid.NewUUID())

		By(fmt.Sprintf("Creating a zonal PD %s in zone %s", volName, zone))
		// Using any client for CreateVolume, since all of them point to the same GCP project
		resp, err := client1.CreateVolume(volName, map[string]string{
			parameters.ParameterKeyType: "pd-standard",
		}, defaultSizeGb, &csi.TopologyRequirement{
			Requisite: []*csi.Topology{
				{Segments: map[string]string{constants.TopologyKeyZone: zone}},
			},
			Preferred: []*csi.Topology{
				{Segments: map[string]string{constants.TopologyKeyZone: zone}},
			},
		}, nil)
		Expect(err).To(BeNil(), "Failed to create volume %s in zone %s: %v", volName, zone, err)
		volID := resp.GetVolumeId()
		Expect(volID).ToNot(BeEmpty(), "Volume ID was empty")

		defer func() {
			By("Cleaning up volume")
			_ = client1.DeleteVolume(volID)
		}()

		stageDir1 := "/tmp/stage1-" + string(uuid.NewUUID())
		mountDir1 := "/tmp/mount1-" + string(uuid.NewUUID())
		stageDir2 := "/tmp/stage2-" + string(uuid.NewUUID())
		mountDir2 := "/tmp/mount2-" + string(uuid.NewUUID())
		testFile := "rox-test"
		testContent := "ROX data"

		By("ControllerPublishVolume RW to node1")
		err = client1.ControllerPublishVolumeReadWrite(volID, inst1.GetNodeID(), false)
		Expect(err).To(BeNil())

		By("Waiting for disk to appear on node1")
		err = testutils.WaitForDiskToAppear(inst1, volName)
		Expect(err).To(BeNil())

		By("NodeStageVolume on node1")
		err = client1.NodeStageExt4Volume(volID, stageDir1, false)
		Expect(err).To(BeNil())

		By("NodePublishVolume RW on node1")
		err = client1.NodePublishVolume(volID, stageDir1, mountDir1)
		Expect(err).To(BeNil())

		By("Writing data from node1")
		err = testutils.WriteFileWithSudo(inst1,filepath.Join(mountDir1, testFile),testContent)
		Expect(err).To(BeNil())

		By("Checking mount and file on node1")
		mountOut, _ := inst1.SSH("mount | grep " + mountDir1)
		klog.Infof("Mount output on node1: %s", mountOut)
		lsOut, _ := inst1.SSH("ls -l " + mountDir1)
		klog.Infof("ls output on node1: %s", lsOut)

		written, err := testutils.ReadFileWithSudo(inst1,filepath.Join(mountDir1, testFile))
		Expect(err).To(BeNil(), "Failed to read file from node1: output=%q", written)
		Expect(strings.TrimSpace(string(written))).To(Equal(testContent), "File content mismatch on node1. Read: %q", written)

		// Unpublish RW from node1 to allow ROX on multiple nodes
		By("Unpublishing RW from node1")
		err = client1.NodeUnpublishVolume(volID, mountDir1)
		Expect(err).To(BeNil())
		err = client1.NodeUnstageVolume(volID, stageDir1)
		Expect(err).To(BeNil())
		err = client1.ControllerUnpublishVolume(volID, inst1.GetNodeID())
		Expect(err).To(BeNil())

		// Attach ROX to node1 and node2
		
		By("ControllerPublishVolume ROX to node1")
		err = client1.ControllerPublishVolumeReadOnly(volID, inst1.GetNodeID())
		Expect(err).To(BeNil())

		By("ControllerPublishVolume ROX to node2")
		err = client2.ControllerPublishVolumeReadOnly(volID, inst2.GetNodeID())
		Expect(err).To(BeNil())

		By("Waiting for disk to appear on node1")
		err = testutils.WaitForDiskToAppear(inst1, volName)
		Expect(err).To(BeNil())

		By("Waiting for disk to appear on node2")
		err = testutils.WaitForDiskToAppear(inst2, volName)
		Expect(err).To(BeNil())

		By("NodeStageVolume ROX on node1")
		err = client1.NodeStageExt4VolumeReadOnly(volID, stageDir1)
		Expect(err).To(BeNil())

		By("NodeStageVolume ROX on node2")
		err = client2.NodeStageExt4VolumeReadOnly(volID, stageDir2)
		Expect(err).To(BeNil())

		By("NodePublishVolume ROX on node1")
		err = client1.NodePublishVolumeReadOnly(volID, stageDir1, mountDir1)
		Expect(err).To(BeNil())

		By("NodePublishVolume ROX on node2")
		err = client2.NodePublishVolumeReadOnly(volID, stageDir2, mountDir2)
		Expect(err).To(BeNil())

		// Verify ROX read works on both nodes
		By("Reading file from node1")
		roContent1, err := testutils.ReadFileWithSudo(
			inst1,
			filepath.Join(mountDir1, testFile),
		)
		Expect(err).To(BeNil())
		Expect(strings.TrimSpace(string(roContent1))).To(Equal(testContent))

		By("Reading file from node2")
		roContent2, err := testutils.ReadFileWithSudo(
			inst2,
			filepath.Join(mountDir2, testFile),
		)
		Expect(err).To(BeNil())
		Expect(strings.TrimSpace(string(roContent2))).To(Equal(testContent))

		By("Ensuring write fails on ROX mount")
		err = testutils.WriteFileWithSudo(inst2,filepath.Join(mountDir2, "should-fail"),"nope")
		Expect(err).ToNot(BeNil())
	})
})