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

	It("Should successfully provision and attach a zonal PD to a specific zone",Label("new"), func() {
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

	It("Should fail to attach a zonal PD to an instance in a different zone",Label("new"), func() {
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

	It("Should successfully failover a regional PD between instances in its replica zones",Label("new"), func() {
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

	It("Should fail to provision a volume when requisite topology cannot be satisfied",Label("new"), func() {
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
		// The error might be from GCE or from the driver's own validation if it checks zones.
		// Usually GCE will return a 400 or 404 which the driver translates.
	})

	It("Should respect preferred topology when requisite topology allows multiple zones", Label("new"), func() {
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

	It("Should successfully provision a regional PD with preferred topology being a subset of requisite",Label("new"), func() {
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

})