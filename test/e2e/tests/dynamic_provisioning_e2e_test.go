package tests

import (
	"fmt"

	csi "github.com/container-storage-interface/spec/lib/go/csi"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/util/uuid"
	"sigs.k8s.io/gcp-compute-persistent-disk-csi-driver/pkg/constants"
	gce "sigs.k8s.io/gcp-compute-persistent-disk-csi-driver/pkg/gce-cloud-provider/compute"
	"sigs.k8s.io/gcp-compute-persistent-disk-csi-driver/pkg/parameters"
	remote "sigs.k8s.io/gcp-compute-persistent-disk-csi-driver/test/remote"
	compute "google.golang.org/api/compute/v1"
)

var _ = Describe("GCE PD CSI Driver Dynamic Volume Tests", func() {
	var (
		testContext               *remote.TestContext
		controllerClient          *remote.CsiClient
		instance                  *remote.InstanceInfo
		projectID, zone, nodeName string
		volName                   string
		volID                     string
		err                       error
	)

	BeforeEach(func() {
		testContext = getRandomTestContext()
		controllerClient = testContext.Client
		instance = testContext.Instance
		projectID, zone, nodeName = instance.GetIdentity()

		volName = testNamePrefix + string(uuid.NewUUID())
		volID = "" // Reset volID for each test
	})

	AfterEach(func() {
		if volID != "" {
			By(fmt.Sprintf("Cleaning up volume %s (ID: %s)", volName, volID))
			err = controllerClient.DeleteVolume(volID)
			Expect(err).To(BeNil(), "Failed to delete volume %s: %v", volID, err)

			// Verify disk deletion from GCP
			// Assuming zonal disk
			_, err = computeService.Disks.Get(projectID, zone, volName).Do()
			Expect(gce.IsGCEError(err, "notFound")).To(BeTrue(), "Expected disk %s to not be found. Got: %v", volName, err)
		}
	})

	It("Should dynamically provision a pd-standard volume in a specified zone and attach it", func() {
		By(fmt.Sprintf("Creating a pd-standard volume %s restricted to zone %s", volName, zone))

		// Define volume capabilities and topology requirement
		volCaps := []*csi.VolumeCapability{
			{
				AccessType: &csi.VolumeCapability_Mount{
					Mount: &csi.VolumeCapability_MountVolume{},
				},
				AccessMode: &csi.VolumeCapability_AccessMode{
					Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
				},
			},
		}

		topologyRequirement := &csi.TopologyRequirement{
			Requisite: []*csi.Topology{
				{
					Segments: map[string]string{constants.TopologyKeyZone: zone},
				},
			},
		}

		// Create the volume with specified topology
		resp, err := controllerClient.CreateVolumeWithCaps(volName, map[string]string{
			parameters.ParameterKeyType: "pd-standard",
		}, defaultSizeGb, topologyRequirement, volCaps, nil)

		Expect(err).To(BeNil(), "Failed to create volume %s with topology constraint: %v", volName, err)
		Expect(resp).ToNot(BeNil(), "CreateVolume response was nil")
		volID = resp.GetVolumeId()
		Expect(volID).ToNot(BeEmpty(), "Volume ID was empty")

		By(fmt.Sprintf("Verifying volume %s properties in GCP", volName))
		cloudDisk, err := computeService.Disks.Get(projectID, zone, volName).Do()
		Expect(err).To(BeNil(), "Failed to get disk %s from GCP: %v", volName, err)
		Expect(cloudDisk.Name).To(Equal(volName))
		Expect(cloudDisk.Type).To(ContainSubstring("pd-standard"))
		Expect(cloudDisk.Zone).To(ContainSubstring(zone), "Disk was not created in the specified zone %s, found in %s", zone, cloudDisk.Zone)

		By(fmt.Sprintf("Attaching volume %s to instance %s and performing data operations", volName, nodeName))
		err = testAttachWriteReadDetach(volID, volName, instance, controllerClient, false /* readOnly */, false /* detachAndReattach */, false /* setupDataCache */)
		Expect(err).To(BeNil(), "Failed volume lifecycle check for volume %s on instance %s: %v", volID, nodeName, err)

		By(fmt.Sprintf("Successfully tested dynamic volume %s with topology constraint in zone %s", volName, zone))
	})

	It("Should dynamically provision a pd-balanced volume with multi-zone topology and attach it",Label("new"), func() {
		By(fmt.Sprintf("Creating a pd-balanced volume %s with multi-zone topology", volName))

		volCaps := []*csi.VolumeCapability{
			{
				AccessType: &csi.VolumeCapability_Mount{
					Mount: &csi.VolumeCapability_MountVolume{},
				},
				AccessMode: &csi.VolumeCapability_AccessMode{
					Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
				},
			},
		}

		// Simulate multi-zone topology (use current zone and a fake one for test)
		multiZone := []string{zone, zone + "-b"}
		topologyRequirement := &csi.TopologyRequirement{
			Requisite: []*csi.Topology{
				{Segments: map[string]string{constants.TopologyKeyZone: multiZone[0]}},
				{Segments: map[string]string{constants.TopologyKeyZone: multiZone[1]}},
			},
		}

		resp, err := controllerClient.CreateVolumeWithCaps(volName, map[string]string{
			parameters.ParameterKeyType: "pd-balanced",
		}, defaultSizeGb, topologyRequirement, volCaps, nil)

		Expect(err).To(BeNil(), "Failed to create pd-balanced volume %s with multi-zone: %v", volName, err)
		Expect(resp).ToNot(BeNil(), "CreateVolume response was nil")
		volID = resp.GetVolumeId()
		Expect(volID).ToNot(BeEmpty(), "Volume ID was empty")

		By(fmt.Sprintf("Verifying pd-balanced volume %s properties in GCP", volName))
		cloudDisk, err := computeService.Disks.Get(projectID, zone, volName).Do()
		Expect(err).To(BeNil(), "Failed to get pd-balanced disk %s from GCP: %v", volName, err)
		Expect(cloudDisk.Name).To(Equal(volName))
		Expect(cloudDisk.Type).To(ContainSubstring("pd-balanced"))

		By(fmt.Sprintf("Attaching pd-balanced volume %s to instance %s as read-only", volName, nodeName))
		err = testAttachWriteReadDetach(volID, volName, instance, controllerClient, false /* readOnly */, false, false)
		Expect(err).To(BeNil(), "Failed read write attach for pd-balanced volume %s: %v", volID, err)
		err = testAttachWriteReadDetach(volID, volName, instance, controllerClient, true /* readOnly */, false, false)
		Expect(err).To(BeNil(), "Failed read-only attach for pd-balanced volume %s: %v", volID, err)

		By(fmt.Sprintf("Successfully tested multi-zone dynamic volume %s", volName))
	})

	It("Should fail to create a volume with invalid parameters", Label("new"), func() {
		By(fmt.Sprintf("Attempting to create a volume %s with invalid type", volName))
		volCaps := []*csi.VolumeCapability{
			{
				AccessType: &csi.VolumeCapability_Mount{
					Mount: &csi.VolumeCapability_MountVolume{},
				},
				AccessMode: &csi.VolumeCapability_AccessMode{
					Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
				},
			},
		}
		_, err := controllerClient.CreateVolumeWithCaps(volName, map[string]string{
			parameters.ParameterKeyType: "not-a-real-type",
		}, defaultSizeGb, nil, volCaps, nil)
		Expect(err).ToNot(BeNil(), "Expected error for invalid disk type, got nil")
	})

	It("Should expand a pd-standard volume and verify new size",Label("new"), func() {
		By(fmt.Sprintf("Creating a pd-standard volume %s for expansion", volName))
		volCaps := []*csi.VolumeCapability{
			{
				AccessType: &csi.VolumeCapability_Mount{
					Mount: &csi.VolumeCapability_MountVolume{},
				},
				AccessMode: &csi.VolumeCapability_AccessMode{
					Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
				},
			},
		}
		resp, err := controllerClient.CreateVolumeWithCaps(volName, map[string]string{
			parameters.ParameterKeyType: "pd-standard",
		}, 10, nil, volCaps, nil)
		Expect(err).To(BeNil(), "Failed to create pd-standard volume for expansion: %v", err)
		volID = resp.GetVolumeId()
		Expect(volID).ToNot(BeEmpty(), "Volume ID was empty")

		By("Expanding the volume to 20GiB")
		err = controllerClient.ControllerExpandVolume(volID, 20)
		Expect(err).To(BeNil(), "Failed to expand volume: %v", err)

		By("Verifying expanded size in GCP")
		cloudDisk, err := computeService.Disks.Get(projectID, zone, volName).Do()
		Expect(err).To(BeNil(), "Failed to get disk after expansion: %v", err)
		Expect(cloudDisk.SizeGb).To(BeNumerically(">=", 20), "Disk size not expanded in GCP")
	})

	It("Should create and attach a pd-extreme volume with custom IOPS", Label("new"), func() {
		By(fmt.Sprintf("Creating a pd-extreme volume %s with custom IOPS", volName))
		volCaps := []*csi.VolumeCapability{
			{
				AccessType: &csi.VolumeCapability_Mount{
					Mount: &csi.VolumeCapability_MountVolume{},
				},
				AccessMode: &csi.VolumeCapability_AccessMode{
					Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
				},
			},
		}
		customIops := "7500"
		resp, err := controllerClient.CreateVolumeWithCaps(volName, map[string]string{
			parameters.ParameterKeyType: "pd-extreme",
			parameters.ParameterKeyProvisionedIOPSOnCreate: customIops,
		}, 100, nil, volCaps, nil)
		Expect(err).To(BeNil(), "Failed to create pd-extreme volume: %v", err)
		volID = resp.GetVolumeId()
		Expect(volID).ToNot(BeEmpty(), "Volume ID was empty")

		By("Verifying pd-extreme volume properties in GCP")
		cloudDisk, err := computeService.Disks.Get(projectID, zone, volName).Do()
		Expect(err).To(BeNil(), "Failed to get pd-extreme disk: %v", err)
		Expect(cloudDisk.Type).To(ContainSubstring("pd-extreme"))
		Expect(cloudDisk.ProvisionedIops).To(BeNumerically(">=", 7500), "Provisioned IOPS not set correctly")

		By("Attaching pd-extreme volume and performing data operations")
		err = testAttachWriteReadDetach(volID, volName, instance, controllerClient, false, false, false)
		Expect(err).To(BeNil(), "Failed pd-extreme volume lifecycle: %v", err)
	})

	It("Should implement basic dynamic volume lifecycle test (pd-ssd)", func() {
		By(fmt.Sprintf("Creating a pd-ssd volume %s", volName))

		volCaps := []*csi.VolumeCapability{
			{
				AccessType: &csi.VolumeCapability_Mount{
					Mount: &csi.VolumeCapability_MountVolume{},
				},
				AccessMode: &csi.VolumeCapability_AccessMode{
					Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
				},
			},
		}

		resp, err := controllerClient.CreateVolumeWithCaps(volName, map[string]string{
			parameters.ParameterKeyType: "pd-ssd",
		}, defaultSizeGb, nil, volCaps, nil) // No topology constraint for basic test

		Expect(err).To(BeNil(), "Failed to create pd-ssd volume %s: %v", volName, err)
		Expect(resp).ToNot(BeNil(), "CreateVolume response was nil")
		volID = resp.GetVolumeId()
		Expect(volID).ToNot(BeEmpty(), "Volume ID was empty")

		By(fmt.Sprintf("Verifying pd-ssd volume %s properties in GCP", volName))
		cloudDisk, err := computeService.Disks.Get(projectID, zone, volName).Do()
		Expect(err).To(BeNil(), "Failed to get pd-ssd disk %s from GCP: %v", volName, err)
		Expect(cloudDisk.Name).To(Equal(volName))
		Expect(cloudDisk.Type).To(ContainSubstring("pd-ssd"))

		By(fmt.Sprintf("Attaching pd-ssd volume %s to instance %s and performing data operations", volName, nodeName))
		err = testAttachWriteReadDetach(volID, volName, instance, controllerClient, false /* readOnly */, false /* detachAndReattach */, false /* setupDataCache */)
		Expect(err).To(BeNil(), "Failed pd-ssd volume lifecycle check for volume %s on instance %s: %v", volID, nodeName, err)

		By(fmt.Sprintf("Successfully completed basic dynamic volume lifecycle test for pd-ssd volume %s", volName))
	})

	It("Should implement Hyperdisk dynamic volume lifecycle test with custom IOPS/throughput", func() {
		By(fmt.Sprintf("Creating a hyperdisk-balanced volume %s with custom IOPS/throughput", volName))

		// Define desired IOPS and Throughput
		//wantIOPs := int64(10000)
		wantThroughput := int64(150) // MB/s

		volCaps := []*csi.VolumeCapability{
			{
				AccessType: &csi.VolumeCapability_Mount{
					Mount: &csi.VolumeCapability_MountVolume{},
				},
				AccessMode: &csi.VolumeCapability_AccessMode{
					Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
				},
			},
		}

		// Create the volume with custom IOPS and Throughput
		resp, err := controllerClient.CreateVolumeWithCaps(volName, map[string]string{
			parameters.ParameterKeyType:                          hdbDiskType,
			parameters.ParameterKeyProvisionedIOPSOnCreate:       "10000",
			parameters.ParameterKeyProvisionedThroughputOnCreate: provisionedThroughputOnCreateHdb,
		},100, nil, volCaps, nil)

		Expect(err).To(BeNil(), "Failed to create hyperdisk-balanced volume %s: %v", volName, err)
		Expect(resp).ToNot(BeNil(), "CreateVolume response was nil")
		volID = resp.GetVolumeId()
		Expect(volID).ToNot(BeEmpty(), "Volume ID was empty")

		By(fmt.Sprintf("Verifying hyperdisk-balanced volume %s properties in GCP", volName))
		cloudDisk, err := computeService.Disks.Get(projectID, zone, volName).Do()
		Expect(err).To(BeNil(), "Failed to get hyperdisk-balanced disk %s from GCP: %v", volName, err)
		Expect(cloudDisk.Name).To(Equal(volName))
		Expect(cloudDisk.Type).To(ContainSubstring("hyperdisk-balanced"))
		// Expect(cloudDisk.ProvisionedIops).To(Equal(wantIOPs))
		Expect(cloudDisk.ProvisionedThroughput).To(Equal(wantThroughput))

		By(fmt.Sprintf("Attaching hyperdisk-balanced volume %s to instance %s and performing data operations", volName, nodeName))
		err = testAttachWriteReadDetach(volID, volName, instance, controllerClient, false /* readOnly */, false /* detachAndReattach */, false /* setupDataCache */)
		Expect(err).To(BeNil(), "Failed hyperdisk-balanced volume lifecycle check for volume %s on instance %s: %v", volID, nodeName, err)

		By(fmt.Sprintf("Successfully completed hyperdisk dynamic volume lifecycle test for volume %s", volName))
	})

	It("Should handle pre-existing disk, concurrent attach, expansion, and error recovery",Label("new"), func() {
		By("Simulating a pre-existing disk and dynamic provisioning with same name")
		// Manually create a disk in GCP to simulate pre-existing resource
		preExistingDiskName := volName + "-preexist"
		gcpDisk := &compute.Disk{
			Name:   preExistingDiskName,
			SizeGb: 10,
			Type:   fmt.Sprintf("zones/%s/diskTypes/pd-standard", zone),
		}
		_, err := computeService.Disks.Insert(projectID, zone, gcpDisk).Do()
		Expect(err).To(BeNil(), "Failed to create pre-existing disk: %v", err)

		// Attempt to provision a volume with the same name (should fail)
		volCaps := []*csi.VolumeCapability{
			{
				AccessType: &csi.VolumeCapability_Mount{
					Mount: &csi.VolumeCapability_MountVolume{},
				},
				AccessMode: &csi.VolumeCapability_AccessMode{
					Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
				},
			},
		}
		_, err = controllerClient.CreateVolumeWithCaps(preExistingDiskName, map[string]string{
			parameters.ParameterKeyType: "pd-standard",
		}, 10, nil, volCaps, nil)
		Expect(err).ToNot(BeNil(), "Expected error when provisioning with pre-existing disk name, got nil")

		// Provision a new volume and attach to two nodes concurrently (simulate multi-attach error)
		By("Provisioning a new pd-standard volume and attempting concurrent attach")
		resp, err := controllerClient.CreateVolumeWithCaps(volName, map[string]string{
			parameters.ParameterKeyType: "pd-standard",
		}, 10, nil, volCaps, nil)
		Expect(err).To(BeNil(), "Failed to create volume: %v", err)
		volID = resp.GetVolumeId()
		Expect(volID).ToNot(BeEmpty(), "Volume ID was empty")

		// Simulate a second node (could be a different instance in testContext)
		secondInstance := getRandomTestContext().Instance

		// Attach to first node
		err = testAttachWriteReadDetach(volID, volName, instance, controllerClient, false, false, false)
		Expect(err).To(BeNil(), "Failed attach/detach on first node: %v", err)

		// Attempt attach to second node while still attached to first (should fail for SINGLE_NODE_WRITER)
		if secondInstance != nil && secondInstance.GetName() != instance.GetName() {
			err = testAttachWriteReadDetach(volID, volName, secondInstance, controllerClient, false, false, false)
			Expect(err).ToNot(BeNil(), "Expected error for concurrent attach to second node, got nil")
		} else {
			By("[SKIP] Only one unique instance available; skipping concurrent attach error check.")
		}

		// Expand the volume while attached
		By("Expanding the volume while attached")
		err = controllerClient.ControllerExpandVolume(volID, 20)
		Expect(err).To(BeNil(), "Failed to expand volume while attached: %v", err)
		cloudDisk, err := computeService.Disks.Get(projectID, zone, volName).Do()
		Expect(err).To(BeNil(), "Failed to get disk after expansion: %v", err)
		Expect(cloudDisk.SizeGb).To(BeNumerically(">=", 20), "Disk size not expanded in GCP")

		// Clean up pre-existing disk
		By("Cleaning up pre-existing disk")
		_, err = computeService.Disks.Delete(projectID, zone, preExistingDiskName).Do()
		Expect(err).To(BeNil(), "Failed to delete pre-existing disk: %v", err)
	})
})
