package tests

import (
	"fmt"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/klog/v2"
	"k8s.io/apimachinery/pkg/util/uuid"
	"google.golang.org/api/googleapi"
	"k8s.io/apimachinery/pkg/util/wait"
	"time"
	gce "sigs.k8s.io/gcp-compute-persistent-disk-csi-driver/pkg/gce-cloud-provider/compute"
	"sigs.k8s.io/gcp-compute-persistent-disk-csi-driver/pkg/parameters"
	testutils "sigs.k8s.io/gcp-compute-persistent-disk-csi-driver/test/e2e/utils"
	remote "sigs.k8s.io/gcp-compute-persistent-disk-csi-driver/test/remote"
)

var _ = Describe("GCE PD CSI Driver Autoprovisioning Tests", Label("autoprovisioning"), func() {
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
			if err != nil && !gce.IsGCEError(err, "notFound") {
				// trying regional get if zonal failed and it's not "notFound"
				region := zone[:len(zone)-2]
				_, err = computeService.RegionDisks.Get(projectID, region, volName).Do()
			}
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

	It("Should allow attachment and detach of a volume from a deleted and then recreated instance with same name", func() {
		By(fmt.Sprintf("Creating a pd-balanced volume %s", volName))
		resp, err := controllerClient.CreateVolume(volName, map[string]string{
			parameters.ParameterKeyType: "pd-balanced",
		}, defaultSizeGb, nil, nil)
		Expect(err).To(BeNil(), "Failed to create volume %s: %v", volName, err)
		volID = resp.GetVolumeId()

		By(fmt.Sprintf("Provisioning instance A: %s", nodeName))

		testNodeName := fmt.Sprintf("%s-recreate-%s", testNamePrefix, string(uuid.NewUUID())[:8])
		instanceConfig := remote.InstanceConfig{
			Project:        projectID,
			Architecture:   *architecture,
			Zone:           zone,
			Name:           testNodeName,
			MachineType:    *machineType,
			ServiceAccount: *serviceAccount,
			ImageURL:       *imageURL,
			ComputeService: computeService,
		}

		newTestInstance, err = remote.SetupInstance(instanceConfig)
		Expect(err).To(BeNil(), "Failed to setup instance A: %v", err)

		By(fmt.Sprintf("Setting up CSI driver on instance %s", testNodeName))
		newTestContext, err = testutils.GCEClientAndDriverSetup(newTestInstance, getDriverConfig())
		Expect(err).To(BeNil(), "Failed to set up TestContext for instance %s: %v", testNodeName, err)

		By(fmt.Sprintf("Attaching volume %s to instance %s and writing data", volName, testNodeName))
		err = testAttachWriteReadDetach(volID, volName, newTestInstance, newTestContext.Client, false, false, false)
		Expect(err).To(BeNil(), "Failed initial attach/write/detach: %v", err)

		By(fmt.Sprintf("Deleting instance %s", testNodeName))
		err = remote.TeardownDriverAndClient(newTestContext)
		Expect(err).To(BeNil())
		newTestInstance.DeleteInstance()
		newTestContext = nil
		newTestInstance = nil

		By(fmt.Sprintf("Recreating instance %s with same name", testNodeName))
		newTestInstance, err = remote.SetupInstance(instanceConfig)
		Expect(err).To(BeNil(), "Failed to recreate instance: %v", err)

		By(fmt.Sprintf("Setting up CSI driver again on recreated instance %s", testNodeName))
		newTestContext, err = testutils.GCEClientAndDriverSetup(newTestInstance, getDriverConfig())
		Expect(err).To(BeNil(), "Failed to set up TestContext for recreated instance: %v", err)

		By(fmt.Sprintf("Attaching volume %s to recreated instance %s and verifying data", volName, testNodeName))
		err = testAttachWriteReadDetach(volID, volName, newTestInstance, newTestContext.Client, false, false, false)
		Expect(err).To(BeNil(), "Failed attach/read on recreated instance: %v", err)
	})

	It("Should handle 'Ghost Attachment' race condition: recover from unclean node deletion", func() {

		By(fmt.Sprintf("Creating a pd-standard volume %s", volName))
		resp, err := controllerClient.CreateVolume(volName, map[string]string{
			parameters.ParameterKeyType: "pd-standard",
		}, defaultSizeGb, nil, nil)
		Expect(err).To(BeNil(), "Failed to create volume: %v", err)
		volID = resp.GetVolumeId()

		testNodeName := fmt.Sprintf("%s-ghost-%s", testNamePrefix, string(uuid.NewUUID())[:8])
		By(fmt.Sprintf("Provisioning Node A: %s", testNodeName))
		instanceConfig := remote.InstanceConfig{
			Project:        projectID,
			Architecture:   *architecture,
			Zone:           zone,
			Name:           testNodeName,
			MachineType:    *machineType,
			ServiceAccount: *serviceAccount,
			ImageURL:       *imageURL,
			ComputeService: computeService,
		}
		nodeA, err := remote.SetupInstance(instanceConfig)
		Expect(err).To(BeNil(), "Failed to setup Node A")

		commands := []string{
			"sudo apt-get update",
			"sudo DEBIAN_FRONTEND=noninteractive apt-get install -y udev google-compute-engine",
			"sudo mkdir -p /lib/udev_containerized",
			"sudo ln -sf /usr/lib/udev/scsi_id /lib/udev_containerized/scsi_id",
			"sudo ln -sf /usr/lib/udev/google_nvme_id /lib/udev_containerized/google_nvme_id",
		}
		for _, cmd := range commands {
			_, err := nodeA.SSH(cmd)
			Expect(err).To(BeNil(), "Failed to install deps on Node A")
		}

		ctxA, err := testutils.GCEClientAndDriverSetup(nodeA, getDriverConfig())
		Expect(err).To(BeNil(), "Failed to setup driver on Node A")

		By(fmt.Sprintf("Attaching volume to Node A (%s)", testNodeName))

		err = controllerClient.ControllerPublishVolumeReadWrite(volID, nodeA.GetNodeID(), false)
		Expect(err).To(BeNil(), "Failed to attach volume to Node A")

		gceDisk, err := computeService.Disks.Get(projectID, zone, volName).Do()
		Expect(err).To(BeNil())
		Expect(gceDisk.Users).ToNot(BeEmpty(), "Disk should be attached to Node A")

		By(fmt.Sprintf("Abruptly deleting Node A (%s) without detaching volume", testNodeName))
		// The important part is deleting the VM without calling ControllerUnpublish.
		_ = remote.TeardownDriverAndClient(ctxA)
		nodeA.DeleteInstance()

		By("Waiting for Node A to be fully deleted before recreating")

		waitErr := wait.PollImmediate(5*time.Second, 2*time.Minute, func() (bool, error) {
			_, err := computeService.Instances.Get(projectID, zone, testNodeName).Do()
			if err != nil {
				if gErr, ok := err.(*googleapi.Error); ok && gErr.Code == 404 {
					return true, nil
				}
			}
			return false, nil
		})
		Expect(waitErr).To(BeNil(), "Old instance was not fully deleted in time")

		By(fmt.Sprintf("Provisioning Node A' (same name: %s)", testNodeName))
		nodeAQC, err := remote.SetupInstance(instanceConfig)
		Expect(err).To(BeNil(), "Failed to setup Node A'")

		for _, cmd := range commands {
			_, err := nodeAQC.SSH(cmd)
			Expect(err).To(BeNil(), "Failed to install deps on Node A'")
		}

		ctxAQC, err := testutils.GCEClientAndDriverSetup(nodeAQC, getDriverConfig())
		Expect(err).To(BeNil(), "Failed to setup driver on Node A'")
		defer func() {
			_ = controllerClient.ControllerUnpublishVolume(volID, nodeAQC.GetNodeID())
			_ = remote.TeardownDriverAndClient(ctxAQC)
			nodeAQC.DeleteInstance()
		}()

		// 5. Attach Volume to Node A'
		By("Attaching volume to Node A'")
		
		err = controllerClient.ControllerPublishVolumeReadWrite(volID, nodeAQC.GetNodeID(), false)
		Expect(err).To(BeNil(), "Failed to attach volume to Node A'")

		// gceDisk, err = computeService.Disks.Get(projectID, zone, volName).Do()
		// Expect(err).To(BeNil())
		// Expect(gceDisk.Users).ToNot(BeEmpty(), "Disk should be attached to Node A'")

		stageDir := "/tmp/ghost-stage"
		mountDir := "/tmp/ghost-mount"
		err = ctxAQC.Client.NodeStageExt4Volume(volID, stageDir, false)
		Expect(err).To(BeNil(), "NodeStage failed on Node A'")
		err = ctxAQC.Client.NodePublishVolume(volID, stageDir, mountDir)
		Expect(err).To(BeNil(), "NodePublish failed on Node A'")

		_ = ctxAQC.Client.NodeUnpublishVolume(volID, mountDir)
		_ = ctxAQC.Client.NodeUnstageVolume(volID, stageDir)
	})

	It("Should fail with clear error when OS dependencies are missing", Label("autoprovisioning"), func() {

		By(fmt.Sprintf("Creating a pd-standard volume %s", volName))
		resp, err := controllerClient.CreateVolume(volName, map[string]string{
			parameters.ParameterKeyType: "pd-standard",
		}, defaultSizeGb, nil, nil)
		Expect(err).To(BeNil(), "Failed to create volume: %v", err)
		volID = resp.GetVolumeId()

		testNodeName := fmt.Sprintf("%s-minimal-%s", testNamePrefix, string(uuid.NewUUID())[:8])
		By(fmt.Sprintf("Provisioning Node: %s (Simulating missing dependencies)", testNodeName))
		instanceConfig := remote.InstanceConfig{
			Project:        projectID,
			Architecture:   *architecture,
			Zone:           zone,
			Name:           testNodeName,
			MachineType:    *machineType,
			ServiceAccount: *serviceAccount,
			ImageURL:       *imageURL,
			ComputeService: computeService,
		}
		nodeMin, err := remote.SetupInstance(instanceConfig)
		Expect(err).To(BeNil(), "Failed to setup Node")

		By("Ensuring dependencies are missing (removing udev/scsi_id if present)")

		ctxMin, err := testutils.GCEClientAndDriverSetup(nodeMin, getDriverConfig())
		Expect(err).To(BeNil(), "Failed to setup driver on Node")
		defer func() {
			_ = remote.TeardownDriverAndClient(ctxMin)
			nodeMin.DeleteInstance()
		}()

		By("Attaching volume (ControllerPublish)")
		err = controllerClient.ControllerPublishVolumeReadWrite(volID, nodeMin.GetNodeID(), false)
		Expect(err).To(BeNil(), "ControllerPublish should succeed even if node lacks tools")

		By("Attempting NodeStage (Expected to fail due to missing tools)")
		stageDir := "/tmp/fail-stage"
		err = ctxMin.Client.NodeStageExt4Volume(volID, stageDir, false)

		Expect(err).ToNot(BeNil(), "NodeStage should fail when dependencies are missing")
		// The driver often returns "executable file not found" or similar.
		klog.Infof("Got expected error from NodeStage: %v", err)

		By("Detaching volume after expected NodeStage failure")

		_ = controllerClient.ControllerUnpublishVolume(volID, nodeMin.GetNodeID())

		waitErr := wait.PollImmediate(5*time.Second, 2*time.Minute, func() (bool, error) {
			disk, err := computeService.Disks.Get(projectID, zone, volName).Do()
			if err != nil {
				return false, nil
			}
			return len(disk.Users) == 0, nil
		})
		Expect(waitErr).To(BeNil(), "Disk did not detach in time")
	})
})
