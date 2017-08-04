package azurefilebroker

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"code.cloudfoundry.org/lager"
	"github.com/Azure/azure-sdk-for-go/arm/storage"
	file "github.com/Azure/azure-sdk-for-go/storage"
	"github.com/Azure/go-autorest/autorest"
	"github.com/Azure/go-autorest/autorest/adal"
)

const (
	creator                     = "creator"
	resourceNotFound            = "StatusCode=404"
	fileRequestTimeoutInSeconds = 60
	locationWestUS              = "westus"
)

var (
	userAgent           = "azurefilebroker"
	encryptionKeySource = "Microsoft.Storage"
)

const (
	AzureCloud        = "AzureCloud"
	AzureChinaCloud   = "AzureChinaCloud"
	AzureGermanCloud  = "AzureGermanCloud"
	AzureUSGovernment = "AzureUSGovernment"
	AzureStack        = "AzureStack"
)

type APIVersions struct {
	Storage         string
	Group           string
	ActiveDirectory string
}

type Environment struct {
	ResourceManagerEndpointURL string
	ActiveDirectoryEndpointURL string
	APIVersions                APIVersions
}

var Environments = map[string]Environment{
	AzureCloud: Environment{
		ResourceManagerEndpointURL: "https://management.azure.com/",
		ActiveDirectoryEndpointURL: "https://login.microsoftonline.com",
		APIVersions: APIVersions{
			Storage:         "2016-05-31",
			Group:           "2016-06-01",
			ActiveDirectory: "2015-06-15",
		},
	},
	AzureChinaCloud: Environment{
		ResourceManagerEndpointURL: "https://management.chinacloudapi.cn/",
		ActiveDirectoryEndpointURL: "https://login.chinacloudapi.cn",
		APIVersions: APIVersions{
			Storage:         "2015-06-15",
			Group:           "2016-06-01",
			ActiveDirectory: "2015-06-15",
		},
	},
	AzureUSGovernment: Environment{
		ResourceManagerEndpointURL: "https://management.usgovcloudapi.net/",
		ActiveDirectoryEndpointURL: "https://login.microsoftonline.com",
		APIVersions: APIVersions{
			Storage:         "2015-06-15",
			Group:           "2016-06-01",
			ActiveDirectory: "2015-06-15",
		},
	},
	AzureGermanCloud: Environment{
		ResourceManagerEndpointURL: "https://management.microsoftazure.de/",
		ActiveDirectoryEndpointURL: "https://login.microsoftonline.de",
		APIVersions: APIVersions{
			Storage:         "2015-06-15",
			Group:           "2016-06-01",
			ActiveDirectory: "2015-06-15",
		},
	},
	AzureStack: Environment{
		APIVersions: APIVersions{
			Storage:         "2015-06-15",
			Group:           "2016-06-01",
			ActiveDirectory: "2015-06-15",
		},
	},
}

type StorageAccount struct {
	SubscriptionID          string
	ResourceGroupName       string
	StorageAccountName      string
	UseHTTPS                bool
	SkuName                 storage.SkuName
	Location                string
	CustomDomainName        string
	UseSubDomain            bool
	EnableEncryption        bool
	IsCreatedStorageAccount bool
	AccessKey               string
	BaseURL                 string
	Connection              AzureStorageAccountSDKClient
}

func NewStorageAccount(logger lager.Logger, configuration Configuration) (*StorageAccount, error) {
	logger = logger.Session("storage-account").WithData(lager.Data{"StorageAccountName": configuration.StorageAccountName})
	storageAccount := StorageAccount{
		SubscriptionID:          configuration.SubscriptionID,
		ResourceGroupName:       configuration.ResourceGroupName,
		StorageAccountName:      configuration.StorageAccountName,
		SkuName:                 storage.StandardRAGRS,
		Location:                locationWestUS,
		UseHTTPS:                true,
		CustomDomainName:        "",
		UseSubDomain:            false,
		EnableEncryption:        false,
		IsCreatedStorageAccount: false,
		Connection:              nil,
	}

	if configuration.UseHTTPS != "" {
		if ret, err := strconv.ParseBool(configuration.UseHTTPS); err == nil {
			storageAccount.UseHTTPS = ret
		}
	}
	if configuration.SkuName != "" {
		storageAccount.SkuName = storage.SkuName(configuration.SkuName)
		if storageAccount.SkuName != storage.StandardGRS && storageAccount.SkuName != storage.StandardLRS && storageAccount.SkuName != storage.StandardRAGRS && storageAccount.SkuName != storage.StandardZRS {
			err := fmt.Errorf("The SkuName %q to create the storage account is invalid. It must be Standard_GRS, Standard_LRS, Standard_RAGRS or Standard_ZRS", configuration.SkuName)
			logger.Error("check-sku-name", err)
			return nil, err
		}
	}
	if configuration.Location != "" {
		storageAccount.Location = configuration.Location
	}
	if configuration.CustomDomainName != "" {
		storageAccount.CustomDomainName = configuration.CustomDomainName
	}
	if configuration.UseSubDomain != "" {
		if ret, err := strconv.ParseBool(configuration.UseSubDomain); err == nil {
			storageAccount.UseSubDomain = ret
		}
	}
	if configuration.EnableEncryption != "" {
		if ret, err := strconv.ParseBool(configuration.EnableEncryption); err == nil {
			storageAccount.EnableEncryption = ret
		}
	}

	return &storageAccount, nil
}

//go:generate counterfeiter -o azurefilebrokerfakes/fake_azure_storage_account_sdk_client.go . AzureStorageAccountSDKClient
type AzureStorageAccountSDKClient interface {
	Exists() (bool, error)
	GetAccessKey() (string, error)
	HasFileShare(fileShareName string) (bool, error)
	CreateFileShare(fileShareName string) error
	DeleteFileShare(fileShareName string) error
	GetShareURL(fileShareName string) (string, error)
}

type AzureStorageConnection struct {
	logger                   lager.Logger
	cloudConfig              *CloudConfig
	StorageAccount           *StorageAccount
	storageManagementClient  *storage.AccountsClient
	storageFileServiceClient *file.Client
}

func NewAzureStorageAccountSDKClient(logger lager.Logger, cloudConfig *CloudConfig, storageAccount *StorageAccount) (AzureStorageAccountSDKClient, error) {
	logger = logger.Session("storage-connection").WithData(lager.Data{"StorageAccountName": storageAccount.StorageAccountName})
	connection := AzureStorageConnection{
		logger:                   logger,
		cloudConfig:              cloudConfig,
		StorageAccount:           storageAccount,
		storageManagementClient:  nil,
		storageFileServiceClient: nil,
	}
	if err := connection.initManagementClient(); err != nil {
		return nil, err
	}
	return &connection, nil
}

func (c *AzureStorageConnection) initManagementClient() error {
	logger := c.logger.Session("init-management-client")
	logger.Info("start")
	defer logger.Info("end")

	environment := c.cloudConfig.Azure.Environment
	tenantID := c.cloudConfig.Azure.TenanID
	clientID := c.cloudConfig.Azure.ClientID
	clientSecret := c.cloudConfig.Azure.ClientSecret
	oauthConfig, err := adal.NewOAuthConfig(Environments[environment].ActiveDirectoryEndpointURL, tenantID)
	if err != nil {
		logger.Error("newO-auth-config", err, lager.Data{
			"Environment":                environment,
			"ActiveDirectoryEndpointURL": Environments[environment].ActiveDirectoryEndpointURL,
			"TenanID":                    tenantID,
		})
		return fmt.Errorf("Error in initManagementClient: %v", err)
	}

	resourceManagerEndpointURL := Environments[environment].ResourceManagerEndpointURL
	spt, err := adal.NewServicePrincipalToken(*oauthConfig, clientID, clientSecret, resourceManagerEndpointURL)
	if err != nil {
		logger.Error("newO-service-principal-token", err, lager.Data{
			"Environment":                environment,
			"resourceManagerEndpointURL": resourceManagerEndpointURL,
			"TenanID":                    tenantID,
			"ClientID":                   clientID,
		})
		return fmt.Errorf("Error in initManagementClient: %v", err)
	}

	client := storage.NewAccountsClientWithBaseURI(resourceManagerEndpointURL, c.StorageAccount.SubscriptionID)
	c.storageManagementClient = &client
	c.storageManagementClient.Authorizer = autorest.NewBearerAuthorizer(spt)
	return nil
}

func (c *AzureStorageConnection) Exists() (bool, error) {
	logger := c.logger.Session("exists")
	logger.Info("start")
	defer logger.Info("end")

	if err := c.getBaseURL(); err != nil {
		if strings.Contains(err.Error(), resourceNotFound) {
			err = nil
		}
		return false, err
	}
	return true, nil
}

func (c *AzureStorageConnection) getBaseURL() error {
	logger := c.logger.Session("get-base-url")
	logger.Info("start")
	defer logger.Info("end")

	if c.StorageAccount.BaseURL == "" {
		result, err := c.getStorageAccountProperties()
		if err != nil {
			logger.Error("get-storage-c-properties", err)
			return err
		}
		c.StorageAccount.BaseURL, err = parseBaseURL(*result.AccountProperties.PrimaryEndpoints.File)
		if err != nil {
			logger.Error("parse-base-url", err)
			return err
		}
	}

	return nil
}

func (c *AzureStorageConnection) getStorageAccountProperties() (storage.Account, error) {
	logger := c.logger.Session("getStorageAccountProperties")
	logger.Info("start")
	defer logger.Info("end")

	result, err := c.storageManagementClient.GetProperties(c.StorageAccount.ResourceGroupName, c.StorageAccount.StorageAccountName)
	return result, err
}

func parseBaseURL(fileEndpoint string) (string, error) {
	re := regexp.MustCompile(`http[s]?://([^\.]*)\.([^\.]*)\.([^/]*).*`)
	result := re.FindStringSubmatch(fileEndpoint)
	if len(result) != 4 {
		return "", fmt.Errorf("Error in parsing baseURL from fileEndpoint: %q", fileEndpoint)
	}
	return result[3], nil
}

func (c *AzureStorageConnection) GetAccessKey() (string, error) {
	logger := c.logger.Session("get-access-key")
	logger.Info("start")
	defer logger.Info("end")

	if c.StorageAccount.AccessKey == "" {
		result, err := c.storageManagementClient.ListKeys(c.StorageAccount.ResourceGroupName, c.StorageAccount.StorageAccountName)
		if err != nil {
			logger.Error("list-keys", err, lager.Data{"ResourceGroupName": c.StorageAccount.ResourceGroupName})
			return "", fmt.Errorf("Failed to list keys: %v", err)
		}
		c.StorageAccount.AccessKey = *(*result.Keys)[0].Value
	}
	return c.StorageAccount.AccessKey, nil
}

func (c *AzureStorageConnection) initFileServiceClient() error {
	logger := c.logger.Session("init-file-service-client")
	logger.Info("start")
	defer logger.Info("end")

	if c.storageFileServiceClient != nil {
		return nil
	}

	if c.StorageAccount.AccessKey == "" {
		if _, err := c.GetAccessKey(); err != nil {
			return err
		}
	}

	if c.StorageAccount.BaseURL == "" {
		if err := c.getBaseURL(); err != nil {
			return err
		}
	}

	environment := c.cloudConfig.Azure.Environment
	apiVersion := Environments[environment].APIVersions.Storage
	client, err := file.NewClient(c.StorageAccount.StorageAccountName, c.StorageAccount.AccessKey, c.StorageAccount.BaseURL, apiVersion, c.StorageAccount.UseHTTPS)
	if err != nil {
		logger.Error("new-file-client", err, lager.Data{
			"baseURL":    c.StorageAccount.BaseURL,
			"apiVersion": apiVersion,
			"UseHTTPS":   c.StorageAccount.UseHTTPS,
		})
		return err
	}
	c.storageFileServiceClient = &client
	c.storageFileServiceClient.AddToUserAgent(userAgent)
	return nil
}

func (c *AzureStorageConnection) HasFileShare(fileShareName string) (bool, error) {
	logger := c.logger.Session("has-file-share").WithData(lager.Data{"FileShareName": fileShareName})
	logger.Info("start")
	defer logger.Info("end")

	if err := c.initFileServiceClient(); err != nil {
		return false, err
	}
	fileService := c.storageFileServiceClient.GetFileService()
	share := fileService.GetShareReference(fileShareName)
	exists, err := share.Exists()
	if err != nil {
		logger.Error("check-file-share-exists", err)
	}
	return exists, err
}

func (c *AzureStorageConnection) CreateFileShare(fileShareName string) error {
	logger := c.logger.Session("create-file-share").WithData(lager.Data{"FileShareName": fileShareName})
	logger.Info("start")
	defer logger.Info("end")

	if err := c.initFileServiceClient(); err != nil {
		return err
	}
	fileService := c.storageFileServiceClient.GetFileService()
	share := fileService.GetShareReference(fileShareName)
	options := file.FileRequestOptions{Timeout: fileRequestTimeoutInSeconds}
	err := share.Create(&options)
	if err != nil {
		logger.Error("create-file-share", err)
	}
	return err
}

func (c *AzureStorageConnection) DeleteFileShare(fileShareName string) error {
	logger := c.logger.Session("delete-file-share").WithData(lager.Data{"FileShareName": fileShareName})
	logger.Info("start")
	defer logger.Info("end")

	if err := c.initFileServiceClient(); err != nil {
		return err
	}
	fileService := c.storageFileServiceClient.GetFileService()
	share := fileService.GetShareReference(fileShareName)
	options := file.FileRequestOptions{Timeout: fileRequestTimeoutInSeconds}
	err := share.Delete(&options)
	if err != nil {
		// TBD: return nil when the share does not exist
		logger.Error("delete-file-share", err)
	}
	return err
}

func (c *AzureStorageConnection) GetShareURL(fileShareName string) (string, error) {
	logger := c.logger.Session("get-share-url").WithData(lager.Data{"FileShareName": fileShareName})
	logger.Info("start")
	defer logger.Info("end")

	if c.StorageAccount.BaseURL == "" {
		if err := c.getBaseURL(); err != nil {
			return "", err
		}
	}

	return fmt.Sprintf("//%s.file.%s/%s", c.StorageAccount.StorageAccountName, c.StorageAccount.BaseURL, fileShareName), nil
}
