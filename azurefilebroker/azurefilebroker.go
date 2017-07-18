package azurefilebroker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"strconv"
	"strings"
	"sync"

	"crypto/md5"

	"code.cloudfoundry.org/clock"
	"code.cloudfoundry.org/goshims/osshim"
	"code.cloudfoundry.org/lager"
	"github.com/pivotal-cf/brokerapi"
)

const (
	permissionVolumeMount = brokerapi.RequiredPermission("volume_mount")
	defaultContainerPath  = "/var/vcap/data"
)

const (
	driverName       string = "smbdriver"
	deviceTypeShared string = "shared"
)

const (
	lockTimeoutInSeconds int = 30
)

type Configuration struct {
	SubscriptionID     string `json:"subscription_id"`
	ResourceGroupName  string `json:"resource_group_name"`
	StorageAccountName string `json:"storage_account_name"`
	UseHTTPS           string `json:"use_https"` // bool
	SkuName            string `json:"sku_name"`
	Location           string `json:"location"`
	CustomDomainName   string `json:"custom_domain_name"`
	UseSubDomain       string `json:"use_sub_domain"`    // bool
	EnableEncryption   string `json:"enable_encryption"` // bool
}

func (config Configuration) Validate() error {
	missingKeys := []string{}
	if config.SubscriptionID == "" {
		missingKeys = append(missingKeys, "subscription_id")
	}
	if config.ResourceGroupName == "" {
		missingKeys = append(missingKeys, "resource_group_name")
	}
	if config.StorageAccountName == "" {
		missingKeys = append(missingKeys, "storage_account_name")
	}

	if len(missingKeys) > 0 {
		return fmt.Errorf("Missing required parameters: %s", strings.Join(missingKeys, ", "))
	}
	return nil
}

type BindOptions struct {
	FileShareName string `json:"share"`
	UID           string `json:"uid"`
	GID           string `json:"gid"`
	FileMode      string `json:"file_mode"`
	DirMode       string `json:"dir_mode"`
	Readonly      bool   `json:"readonly"`
	Vers          string `json:"vers"`
	Mount         string `json:"mount"`
}

// ToMap Omit FileShareName
func (options BindOptions) ToMap() map[string]string {
	ret := make(map[string]string)
	if options.UID != "" {
		ret["uid"] = fmt.Sprintf("%#v", options.UID)
	}
	if options.GID != "" {
		ret["gid"] = fmt.Sprintf("%#v", options.GID)
	}
	if options.FileMode != "" {
		ret["file_mode"] = options.FileMode
	}
	if options.DirMode != "" {
		ret["dir_mode"] = options.DirMode
	}
	if options.Readonly {
		ret["readonly"] = strconv.FormatBool(options.Readonly)
	}
	if options.Vers != "" {
		ret["vers"] = options.Vers
	}
	return ret
}

type staticState struct {
	ServiceName string `json:"ServiceName"`
	ServiceID   string `json:"ServiceID"`
}

type FileShare struct {
	InstanceID    string `json:"instance_id"`
	FileShareName string `json:"file_share_name"`
	IsCreated     bool   `json:"is_created"` // true if it is created by the broker.
	Count         int    `json:"count"`
	URL           string `json:"url"`
}

func getFileShareID(instanceID, fileShareName string) string {
	return fmt.Sprintf("%s-%s", instanceID, fileShareName)
}

type OperationStatus string

const (
	StatusPending    OperationStatus = "Pending"
	StatusInProgress OperationStatus = "InProgress"
	StatusSuccess    OperationStatus = "Success"
)

type ServiceInstance struct {
	ServiceID               string          `json:"service_id"`
	PlanID                  string          `json:"plan_id"`
	OrganizationGUID        string          `json:"organization_guid"`
	SpaceGUID               string          `json:"space_guid"`
	SubscriptionID          string          `json:"subscription_id"`
	ResourceGroupName       string          `json:"resource_group_name"`
	StorageAccountName      string          `json:"storage_account_name"`
	UseHTTPS                string          `json:"use_https"`
	IsCreatedStorageAccount bool            `json:"is_created_storage_account"`
	OperationStatus         OperationStatus `json:"operation_status"`
	OperationURL            string          `json:"operation_url"`
}

type lock interface {
	Lock()
	Unlock()
}

type Broker struct {
	logger lager.Logger
	os     osshim.Os
	mutex  lock
	clock  clock.Clock
	static staticState
	store  Store
	config Config
}

func New(
	logger lager.Logger,
	serviceName, serviceID string,
	os osshim.Os,
	clock clock.Clock,
	store Store,
	config *Config,
) *Broker {
	theBroker := Broker{
		logger: logger,
		os:     os,
		mutex:  &sync.Mutex{},
		clock:  clock,
		static: staticState{
			ServiceName: serviceName,
			ServiceID:   serviceID,
		},
		store:  store,
		config: *config,
	}

	return &theBroker
}

func (b *Broker) Services(_ context.Context) []brokerapi.Service {
	logger := b.logger.Session("services")
	logger.Info("start")
	defer logger.Info("end")

	return []brokerapi.Service{{
		ID:            b.static.ServiceID,
		Name:          b.static.ServiceName,
		Description:   "Existing Azure File SMB volumes (see: https://github.com/AbelHu/smb-volume-release/)",
		Bindable:      true,
		PlanUpdatable: false,
		Tags:          []string{"azurefile", "smb"},
		Requires:      []brokerapi.RequiredPermission{permissionVolumeMount},

		Plans: []brokerapi.ServicePlan{
			{
				Name:        "AzureFileShare",
				ID:          "06948cb0-cad7-4buh-leba-9ed8b5c345a4",
				Description: "Azure File Share",
			},
		},
	}}
}

// Provision Create a service instance which is mapped to a storage account
// TBD: Now this broker does not support to create storage account if it does not exist
// Now ControlConfig.AllowCreateStorageAccount is always set to false
func (b *Broker) Provision(context context.Context, instanceID string, details brokerapi.ProvisionDetails, asyncAllowed bool) (_ brokerapi.ProvisionedServiceSpec, e error) {
	logger := b.logger.Session("provision").WithData(lager.Data{"instanceID": instanceID, "details": details, "asyncAllowed": asyncAllowed})
	logger.Info("start")
	defer logger.Info("end")

	var configuration Configuration
	var decoder = json.NewDecoder(bytes.NewBuffer(details.RawParameters))
	if err := decoder.Decode(&configuration); err != nil {
		logger.Error("decode-configuration", err)
		return brokerapi.ProvisionedServiceSpec{}, brokerapi.ErrRawParamsInvalid
	}

	if configuration.SubscriptionID == "" {
		configuration.SubscriptionID = b.config.cloud.Azure.DefaultSubscriptionID
	}
	if configuration.ResourceGroupName == "" {
		configuration.ResourceGroupName = b.config.cloud.Azure.DefaultResourceGroupName
	}

	if err := configuration.Validate(); err != nil {
		logger.Error("validate-configuration", err)
		return brokerapi.ProvisionedServiceSpec{}, err
	}

	b.mutex.Lock()
	defer b.mutex.Unlock()

	storageAccount, err := b.getStorageAccount(logger, configuration)
	if err != nil {
		logger.Error("get-storage-account", err)
		return brokerapi.ProvisionedServiceSpec{}, err
	}

	serviceInstance := ServiceInstance{
		details.ServiceID,
		details.PlanID,
		details.OrganizationGUID,
		details.SpaceGUID,
		storageAccount.SubscriptionID,
		storageAccount.ResourceGroupName,
		storageAccount.StorageAccountName,
		strconv.FormatBool(storageAccount.UseHTTPS),
		storageAccount.IsCreatedStorageAccount,
		StatusSuccess,
		"",
	}

	err = b.store.CreateServiceInstance(instanceID, serviceInstance)
	if err != nil {
		logger.Error("create-service-instance", err, lager.Data{"serviceInstance": serviceInstance})
		return brokerapi.ProvisionedServiceSpec{}, fmt.Errorf("Failed to store instance details %q: %s", instanceID, err)
	}

	logger.Debug("service-instance-created", lager.Data{"serviceInstance": serviceInstance})

	return brokerapi.ProvisionedServiceSpec{IsAsync: false}, nil
}

func (b *Broker) getStorageAccount(logger lager.Logger, configuration Configuration) (*StorageAccount, error) {
	logger = logger.Session("get-storage-account")
	logger.Info("start")
	defer logger.Info("end")

	storageAccount, err := NewStorageAccount(
		logger,
		&b.config.cloud,
		configuration)
	if err != nil {
		return nil, err
	}

	if exist, err := storageAccount.Exists(); err != nil {
		return nil, fmt.Errorf("Failed to check whether storage account exists: %v", err)
	} else if exist {
		logger.Debug("check-storage-account-exist", lager.Data{
			"message": fmt.Sprintf("The storage account %q exists.", storageAccount.StorageAccountName),
		})
		return storageAccount, nil
	} else if !b.config.cloud.Control.AllowCreateStorageAccount {
		return nil, fmt.Errorf("The storage account %q does not exist under the resource group %q in the subscription %q and the administrator does not allow to create it automatically", storageAccount.StorageAccountName, storageAccount.ResourceGroupName, storageAccount.SubscriptionID)
	}

	if err := storageAccount.Create(); err != nil {
		return nil, fmt.Errorf("Failed to create the storage account %q under the resource group %q in the subscription %q: %v", storageAccount.StorageAccountName, storageAccount.ResourceGroupName, storageAccount.SubscriptionID, err)
	}
	storageAccount.IsCreatedStorageAccount = true

	return storageAccount, nil
}

func (b *Broker) Deprovision(context context.Context, instanceID string, details brokerapi.DeprovisionDetails, asyncAllowed bool) (_ brokerapi.DeprovisionServiceSpec, e error) {
	logger := b.logger.Session("deprovision").WithData(lager.Data{"instanceID": instanceID, "details": details, "asyncAllowed": asyncAllowed})
	logger.Info("start")
	defer logger.Info("end")

	b.mutex.Lock()
	defer b.mutex.Unlock()

	serviceInstance, err := b.store.RetrieveServiceInstance(instanceID)
	if err != nil {
		logger.Error("retrieve-service-instance", err)
		return brokerapi.DeprovisionServiceSpec{}, brokerapi.ErrInstanceDoesNotExist
	}

	if serviceInstance.IsCreatedStorageAccount && b.config.cloud.Control.AllowDeleteStorageAccount {
		storageAccount, err := NewStorageAccount(
			logger,
			&b.config.cloud,
			Configuration{
				SubscriptionID:     serviceInstance.SubscriptionID,
				ResourceGroupName:  serviceInstance.ResourceGroupName,
				StorageAccountName: serviceInstance.StorageAccountName,
				UseHTTPS:           serviceInstance.UseHTTPS,
			})
		if err != nil {
			return brokerapi.DeprovisionServiceSpec{}, err
		}
		if ok, err := storageAccount.Exists(); err != nil {
			return brokerapi.DeprovisionServiceSpec{}, fmt.Errorf("Failed to delete the storage account %q under the resource group %q in the subscription %q: %v", serviceInstance.StorageAccountName, serviceInstance.ResourceGroupName, serviceInstance.SubscriptionID, err)
		} else if ok {
			if err := storageAccount.Delete(); err != nil {
				return brokerapi.DeprovisionServiceSpec{}, fmt.Errorf("Failed to delete the storage account %q under the resource group %q in the subscription %q: %v", serviceInstance.StorageAccountName, serviceInstance.ResourceGroupName, serviceInstance.SubscriptionID, err)
			}
		}
	}

	err = b.store.DeleteServiceInstance(instanceID)
	if err != nil {
		return brokerapi.DeprovisionServiceSpec{}, err
	}

	logger.Debug("service-instance-deleted", lager.Data{"serviceInstance": serviceInstance})

	return brokerapi.DeprovisionServiceSpec{IsAsync: false, OperationData: "deprovision"}, nil
}

func (b *Broker) Bind(context context.Context, instanceID string, bindingID string, details brokerapi.BindDetails) (_ brokerapi.Binding, e error) {
	logger := b.logger.Session("bind").WithData(lager.Data{"instanceID": instanceID, "bindingID": bindingID, "details": details})
	logger.Info("start")
	defer logger.Info("end")

	if details.AppGUID == "" {
		err := brokerapi.ErrAppGuidNotProvided
		logger.Error("missing-app-guid-parameter", err)
		return brokerapi.Binding{}, err
	}

	var bindOptions BindOptions
	var decoder = json.NewDecoder(bytes.NewBuffer(details.RawParameters))
	if err := decoder.Decode(&bindOptions); err != nil {
		logger.Error("decode-bind-raw-parameters", err, lager.Data{
			"RawParameters:": details.RawParameters,
		})
		return brokerapi.Binding{}, brokerapi.ErrRawParamsInvalid
	}
	if bindOptions.FileShareName == "" {
		err := fmt.Errorf("Missing required parameters: \"share\"")
		logger.Error("missing-share-parameter", err)
		return brokerapi.Binding{}, err
	}
	fileShareName := bindOptions.FileShareName

	globalMountConfig := b.config.mount.Copy()
	if err := globalMountConfig.SetEntries(bindOptions.ToMap()); err != nil {
		logger.Error("set-mount-entries", err, lager.Data{
			"share":       fileShareName,
			"bindOptions": bindOptions,
			"mount":       globalMountConfig.MakeConfig(),
		})
		return brokerapi.Binding{}, err
	}

	b.mutex.Lock()
	defer b.mutex.Unlock()

	serviceInstance, err := b.store.RetrieveServiceInstance(instanceID)
	if err != nil {
		err := brokerapi.ErrInstanceDoesNotExist
		logger.Error("retrieve-service-instance", err)
		return brokerapi.Binding{}, err
	}

	fileShareID := getFileShareID(instanceID, fileShareName)
	err = b.store.GetLockForUpdate(fileShareID, lockTimeoutInSeconds)
	if err != nil {
		logger.Error("get-lock-for-update", err)
		return brokerapi.Binding{}, err
	}
	defer b.store.ReleaseLockForUpdate(fileShareID)

	fileShare, err := b.store.RetrieveFileShare(fileShareID)
	if err != nil {
		if err != brokerapi.ErrInstanceDoesNotExist {
			logger.Error("retrieve-file-share", err)
			return brokerapi.Binding{}, err
		}

		logger.Info("retrieve-file-share", lager.Data{"message": fmt.Sprintf("%s does not exist", fileShareID)})
		fileShare = FileShare{
			InstanceID:    instanceID,
			FileShareName: fileShareName,
			IsCreated:     false,
			Count:         0,
			URL:           "",
		}
		err = nil
	}
	storageAccount, err := b.handleBindShare(logger, &serviceInstance, &fileShare)
	if err != nil {
		return brokerapi.Binding{}, err
	}

	if fileShare.Count == 1 {
		logger.Info("inserting-file-share-into-store", lager.Data{"fileShare": fileShare})
		if err := b.store.CreateFileShare(fileShareID, fileShare); err != nil {
			err = fmt.Errorf("Faied to insert file share into the store for %q: %v", fileShareID, err)
			logger.Error("insert-file-share-into-store", err)
			return brokerapi.Binding{}, err
		}
		logger.Info("inserted-file-share-into-store", lager.Data{"fileShare": fileShare})
	} else {
		logger.Info("updating-file-share-in-store", lager.Data{"fileShare": fileShare})
		if err := b.store.UpdateFileShare(fileShareID, fileShare); err != nil {
			err = fmt.Errorf("Faied to update file share in the store for %q: %v", fileShareID, err)
			logger.Error("update-file-share-in-store", err)
			return brokerapi.Binding{}, err
		}
		logger.Info("updated-file-share-in-store", lager.Data{"fileShare": fileShare})
	}

	err = b.store.CreateBindingDetails(bindingID, details)
	if err != nil {
		logger.Error("create-binding-details", err)
		return brokerapi.Binding{}, err
	}

	logger.Info("binding-details-created")

	mountConfig := globalMountConfig.MakeConfig()
	mountConfig["source"] = fileShare.URL
	mountConfig["username"] = serviceInstance.StorageAccountName

	logger.Debug("volume-service-binding", lager.Data{"driver": "azurefiledriver", "mountConfig": mountConfig, "share": fileShare})

	s, err := b.hash(mountConfig)
	if err != nil {
		logger.Error("error-calculating-volume-id", err, lager.Data{"config": mountConfig})
		return brokerapi.Binding{}, err
	}

	accessKey, err := storageAccount.GetAccessKey()
	if err != nil {
		return brokerapi.Binding{}, err
	}
	mountConfig["password"] = accessKey

	volumeID := fmt.Sprintf("%s-%s", instanceID, s)

	ret := brokerapi.Binding{
		Credentials: struct{}{}, // if nil, cloud controller chokes on response
		VolumeMounts: []brokerapi.VolumeMount{{
			ContainerDir: evaluateContainerPath(bindOptions, instanceID),
			Mode:         readOnlyToMode(bindOptions.Readonly),
			Driver:       driverName,
			DeviceType:   deviceTypeShared,
			Device: brokerapi.SharedDevice{
				VolumeId:    volumeID,
				MountConfig: mountConfig,
			},
		}},
	}

	return ret, nil
}

func (b *Broker) handleBindShare(logger lager.Logger, serviceInstance *ServiceInstance, share *FileShare) (*StorageAccount, error) {
	logger = logger.Session("handle-bind-share").WithData(lager.Data{"FileShareName": share.FileShareName})
	logger.Info("start")
	defer logger.Info("end")

	storageAccount, err := NewStorageAccount(
		logger,
		&b.config.cloud,
		Configuration{
			SubscriptionID:     serviceInstance.SubscriptionID,
			ResourceGroupName:  serviceInstance.ResourceGroupName,
			StorageAccountName: serviceInstance.StorageAccountName,
			UseHTTPS:           serviceInstance.UseHTTPS,
		})
	if err != nil {
		return nil, err
	}

	exist, err := storageAccount.HasFileShare(share.FileShareName)
	if err != nil {
		return nil, fmt.Errorf("Failed to check whether the file share %q exists: %v", share.FileShareName, err)
	}

	if exist {
		share.Count++
		if share.URL == "" {
			shareURL, err := storageAccount.GetShareURL(share.FileShareName)
			if err != nil {
				return nil, err
			}
			share.URL = shareURL
		}
		logger.Debug("file-share-get", lager.Data{"share": share})
	} else {
		if !b.config.cloud.Control.AllowCreateFileShare {
			return nil, fmt.Errorf("The file share %q does not exist in the storage account %q and the administrator does not allow to create it automatically", share.FileShareName, storageAccount.StorageAccountName)
		}
		if err := storageAccount.CreateFileShare(share.FileShareName); err != nil {
			return nil, fmt.Errorf("Failed to create file share %q in the storage account %q: %v", share.FileShareName, storageAccount.StorageAccountName, err)
		}
		share.IsCreated = true
		share.Count = 1
		shareURL, err := storageAccount.GetShareURL(share.FileShareName)
		if err != nil {
			return nil, err
		}
		share.URL = shareURL
		logger.Debug("file-share-created", lager.Data{"share": share})
	}

	return storageAccount, nil
}

func (b *Broker) hash(mountConfig map[string]interface{}) (string, error) {
	var (
		bytes []byte
		err   error
	)
	if bytes, err = json.Marshal(mountConfig); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", md5.Sum(bytes)), nil
}

func (b *Broker) Unbind(context context.Context, instanceID string, bindingID string, details brokerapi.UnbindDetails) (e error) {
	logger := b.logger.Session("unbind").WithData(lager.Data{"instanceID": instanceID, "bindingID": bindingID, "details": details})
	logger.Info("start")
	defer logger.Info("end")

	b.mutex.Lock()
	defer b.mutex.Unlock()

	serviceInstance, err := b.store.RetrieveServiceInstance(instanceID)
	if err != nil {
		logger.Error("retrieve-service-instance", err)
		return brokerapi.ErrInstanceDoesNotExist
	}

	bindDetails, err := b.store.RetrieveBindingDetails(bindingID)
	if err != nil {
		logger.Error("retrieve-binding-details", err)
		return brokerapi.ErrBindingDoesNotExist
	}

	var bindOptions BindOptions
	var decoder = json.NewDecoder(bytes.NewBuffer(bindDetails.RawParameters))
	if err := decoder.Decode(&bindOptions); err != nil {
		logger.Error("decode-bind-raw-parameters", err)
		return brokerapi.ErrRawParamsInvalid
	}
	fileShareName := bindOptions.FileShareName

	fileShareID := getFileShareID(instanceID, fileShareName)
	err = b.store.GetLockForUpdate(fileShareID, lockTimeoutInSeconds)
	if err != nil {
		logger.Error("get-lock-for-update", err)
		return err
	}
	defer b.store.ReleaseLockForUpdate(fileShareID)

	fileShare, err := b.store.RetrieveFileShare(fileShareID)
	if err != nil {
		logger.Error("retrieve-file-share", err)
		return err
	}

	if err := b.handleUnbindShare(logger, &serviceInstance, &fileShare); err != nil {
		return nil
	}

	if fileShare.Count > 0 {
		logger.Debug("updating-file-share-in-store", lager.Data{"fileShare": fileShare})
		if err := b.store.UpdateFileShare(fileShareID, fileShare); err != nil {
			err = fmt.Errorf("Faied to update file share in the store for %q: %v", fileShareID, err)
			logger.Error("update-file-share-in-store", err)
			return err
		}
		logger.Debug("updated-file-share-in-store", lager.Data{"fileShare": fileShare})
	} else {
		logger.Debug("deleting-file-share-from-store", lager.Data{"fileShare": fileShare})
		if err := b.store.DeleteFileShare(fileShareID); err != nil {
			err = fmt.Errorf("Faied to delete file share from the store for %q: %v", fileShareID, err)
			logger.Error("delete-file-share-from-store", err)
			return err
		}
		logger.Debug("deleted-file-share-from-store", lager.Data{"fileShare": fileShare})
	}

	if err := b.store.DeleteBindingDetails(bindingID); err != nil {
		return err
	}

	return nil
}

func (b *Broker) handleUnbindShare(logger lager.Logger, serviceInstance *ServiceInstance, share *FileShare) error {
	logger = logger.Session("handle-unbind-share").WithData(lager.Data{"FileShareName": share.FileShareName})
	logger.Info("start")
	defer logger.Info("end")

	share.Count--
	if share.Count > 0 {
		return nil
	}

	if share.IsCreated && b.config.cloud.Control.AllowDeleteFileShare {
		storageAccount, err := NewStorageAccount(
			logger,
			&b.config.cloud,
			Configuration{
				SubscriptionID:     serviceInstance.SubscriptionID,
				ResourceGroupName:  serviceInstance.ResourceGroupName,
				StorageAccountName: serviceInstance.StorageAccountName,
				UseHTTPS:           serviceInstance.UseHTTPS,
			})
		if err != nil {
			return err
		}

		if err := storageAccount.DeleteFileShare(share.FileShareName); err != nil {
			return fmt.Errorf("Faied to delete the file share %q in the storage account %q: %v", share.FileShareName, serviceInstance.StorageAccountName, err)
		}
	}

	return nil
}

func (b *Broker) Update(context context.Context, instanceID string, details brokerapi.UpdateDetails, asyncAllowed bool) (brokerapi.UpdateServiceSpec, error) {
	panic("not implemented")
}

func (b *Broker) LastOperation(_ context.Context, instanceID string, operationData string) (brokerapi.LastOperation, error) {
	logger := b.logger.Session("last-operation").WithData(lager.Data{"instanceID": instanceID})
	logger.Info("start")
	defer logger.Info("end")

	b.mutex.Lock()
	defer b.mutex.Unlock()

	switch operationData {
	default:
		return brokerapi.LastOperation{}, errors.New("unrecognized operationData")
	}
}

func readOnlyToMode(ro bool) string {
	if ro {
		return "r"
	}
	return "rw"
}

func evaluateContainerPath(options BindOptions, volID string) string {
	if options.Mount != "" {
		return options.Mount
	}

	return path.Join(defaultContainerPath, volID)
}
