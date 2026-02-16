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
		By(fmt.Sprintf("Creating a hyperdisk-throughput volume %s with custom IOPS/throughput", volName))

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
		Expect(cloudDisk.ProvisionedIops).To(Equal(wantIOPs))
		Expect(cloudDisk.ProvisionedThroughput).To(Equal(wantThroughput))

		By(fmt.Sprintf("Attaching hyperdisk-throughput volume %s to instance %s and performing data operations", volName, nodeName))
		err = testAttachWriteReadDetach(volID, volName, instance, controllerClient, false /* readOnly */, false /* detachAndReattach */, false /* setupDataCache */)
		Expect(err).To(BeNil(), "Failed hyperdisk-throughput volume lifecycle check for volume %s on instance %s: %v", volID, nodeName, err)

		By(fmt.Sprintf("Successfully completed hyperdisk dynamic volume lifecycle test for volume %s", volName))
	})
})
