package tests

import (
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/util/uuid"
	gce "sigs.k8s.io/gcp-compute-persistent-disk-csi-driver/pkg/gce-cloud-provider/compute"
	"sigs.k8s.io/gcp-compute-persistent-disk-csi-driver/pkg/parameters"
	testutils "sigs.k8s.io/gcp-compute-persistent-disk-csi-driver/test/e2e/utils"
	remote "sigs.k8s.io/gcp-compute-persistent-disk-csi-driver/test/remote"
)

var _ = Describe("GCE PD CSI Driver Autoprovisioning Tests", func() {
	var (
		testContext               *remote.TestContext
		controllerClient          *remote.CsiClient
		instance                  *remote.InstanceInfo
		projectID, zone, nodeName string
		volName                   string
		volID                     string
		err                       error
		newTestInstance           *remote.InstanceInfo
		newTestContext            *remote.TestContext
	)

	BeforeEach(func() {
		// Use a random test context for initial setup
		testContext = getRandomTestContext()
		controllerClient = testContext.Client
		instance = testContext.Instance
		projectID, zone, nodeName = instance.GetIdentity()

		volName = testNamePrefix + string(uuid.NewUUID())
		volID = "" // Reset volID for each test
		newTestInstance = nil
		newTestContext = nil
	})

	AfterEach(func() {
		if volID != "" {
			By(fmt.Sprintf("Cleaning up volume %s (ID: %s)", volName, volID))
			err = controllerClient.DeleteVolume(volID)
			Expect(err).To(BeNil(), "Failed to delete volume %s: %v", volID, err)

			_, err = computeService.Disks.Get(projectID, zone, volName).Do()
			Expect(gce.IsGCEError(err, "notFound")).To(BeTrue(), "Expected disk %s to not be found. Got: %v", volName, err)
		}
		if newTestContext != nil {
			By(fmt.Sprintf("Cleaning up new test context for instance %s", newTestContext.Instance.GetName()))
			err := remote.TeardownDriverAndClient(newTestContext)
			Expect(err).To(BeNil(), "Failed to teardown new test context")
			newTestContext.Instance.DeleteInstance()
			//Expect(err).To(BeNil(), "Failed to delete new instance")
		}
	})

	It("Should allow attachment of an existing volume to a newly provisioned instance", func() {
		By(fmt.Sprintf("Creating a pd-standard volume %s", volName))
		resp, err := controllerClient.CreateVolume(volName, map[string]string{
			parameters.ParameterKeyType: "pd-standard",
		}, defaultSizeGb, nil, nil)

		Expect(err).To(BeNil(), "Failed to create volume %s: %v", volName, err)
		volID = resp.GetVolumeId()
		Expect(volID).ToNot(BeEmpty(), "Volume ID was empty")

		By(fmt.Sprintf("Provisioning a new GCE instance in zone %s", zone))
		newNodeID := fmt.Sprintf("%s-new-node-%s", testNamePrefix, string(uuid.NewUUID())[:8])
		instanceConfig := remote.InstanceConfig{
			Project:                   projectID,
			Architecture:              *architecture,
			MinCpuPlatform:            *minCpuPlatform,
			Zone:                      zone,
			Name:                      newNodeID,
			MachineType:               *machineType,
			ServiceAccount:            *serviceAccount,
			ImageURL:                  *imageURL,
			CloudtopHost:              *cloudtopHost,
			EnableConfidentialCompute: *enableConfidentialCompute,
			ComputeService:            computeService,
			LocalSSDCount:             0,
		}

		newTestInstance, err = remote.SetupInstance(instanceConfig)
		Expect(err).To(BeNil(), "Failed to setup new instance %s: %v", newNodeID, err)

		// 🔥 Prepare ubuntu-minimal for CSI driver
		By("Installing required udev and GCE utilities on new instance")

		commands := []string{
			"sudo apt-get update",
			"sudo DEBIAN_FRONTEND=noninteractive apt-get install -y udev google-compute-engine",
			"sudo mkdir -p /lib/udev_containerized",
			"sudo ln -sf /usr/lib/udev/scsi_id /lib/udev_containerized/scsi_id",
			"sudo ln -sf /usr/lib/udev/google_nvme_id /lib/udev_containerized/google_nvme_id",
		}

		for _, cmd := range commands {
			_, err := newTestInstance.SSH(cmd)
			Expect(err).To(BeNil(), "Failed running command '%s' on %s: %v", cmd, newNodeID, err)
		}

		By(fmt.Sprintf("Setting up CSI driver and client on new instance %s", newTestInstance.GetName()))
		newTestContext, err = testutils.GCEClientAndDriverSetup(newTestInstance, getDriverConfig())
		Expect(err).To(BeNil(), "Failed to set up TestContext for new instance %s: %v", newTestInstance.GetName(), err)

		By(fmt.Sprintf("Attaching volume %s to newly provisioned instance %s and performing data operations",
			volName, newTestInstance.GetName()))

		err = testAttachWriteReadDetach(
			volID,
			volName,
			newTestInstance,
			newTestContext.Client,
			false, /* readOnly */
			false, /* detachAndReattach */
			false, /* setupDataCache */
		)
		Expect(err).To(BeNil(),
			"Failed volume lifecycle check for volume %s on new instance %s: %v",
			volID, newTestInstance.GetName(), err)

		By(fmt.Sprintf("Successfully tested volume attachment to a newly provisioned instance %s",
			newTestInstance.GetName()))
	})

	It("Should provision a volume with default parameters when only size is specified (autoprovisioning aspects)", func() {
		By(fmt.Sprintf("Creating a volume %s with only size specified", volName))

		// Create the volume with only name and size, omitting type
		resp, err := controllerClient.CreateVolume(volName, nil, defaultSizeGb, nil, nil)

		Expect(err).To(BeNil(), "Failed to create volume %s with only size: %v", volName, err)
		volID = resp.GetVolumeId()
		Expect(volID).ToNot(BeEmpty(), "Volume ID was empty")

		By(fmt.Sprintf("Verifying volume %s properties in GCP for default type", volName))
		cloudDisk, err := computeService.Disks.Get(projectID, zone, volName).Do()
		Expect(err).To(BeNil(), "Failed to get disk %s from GCP: %v", volName, err)
		Expect(cloudDisk.Name).To(Equal(volName))

		// Assuming pd-standard is the default type if not specified
		// This assertion might need to be adjusted if the default behavior changes
		Expect(cloudDisk.Type).To(ContainSubstring("pd-standard"), "Expected default disk type to be pd-standard, got %s", cloudDisk.Type)
		Expect(cloudDisk.SizeGb).To(Equal(defaultSizeGb))

		By(fmt.Sprintf("Attaching volume %s to instance %s and performing data operations", volName, nodeName))
		err = testAttachWriteReadDetach(volID, volName, instance, controllerClient, false /* readOnly */, false /* detachAndReattach */, false /* setupDataCache */)
		Expect(err).To(BeNil(), "Failed volume lifecycle check for volume %s on instance %s: %v", volID, nodeName, err)

		By(fmt.Sprintf("Successfully tested autoprovisioning aspect for volume %s", volName))
	})
})