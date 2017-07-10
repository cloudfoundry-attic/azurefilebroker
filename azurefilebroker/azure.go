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
	creator          = "creator"
	resourceNotFound = "StatusCode=404"
	timeoutInSeconds = 60
	locationWestUS   = "westus"
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
	logger                   lager.Logger
	Environment              string
	TenanID                  string
	ClientID                 string
	ClientSecret             string
	SubscriptionID           string
	ResourceGroupName        string
	StorageAccountName       string
	UseHTTPS                 bool
	SkuName                  storage.SkuName
	Location                 string
	CustomDomainName         string
	UseSubDomain             bool
	EnableEncryption         bool
	IsCreatedStorageAccount  bool
	accessKey                string
	baseURL                  string
	storageManagementClient  *storage.AccountsClient
	storageFileServiceClient *file.Client
}

func NewStorageAccount(logger lager.Logger, environment, tenantID, clientID, clientSecret string, configuration Configuration) (*StorageAccount, error) {
	logger = logger.Session("storage-account").WithData(lager.Data{"StorageAccountName": configuration.StorageAccountName})
	storageAccount := StorageAccount{
		logger:                   logger,
		Environment:              environment,
		TenanID:                  tenantID,
		ClientID:                 clientID,
		ClientSecret:             clientSecret,
		SubscriptionID:           configuration.SubscriptionID,
		ResourceGroupName:        configuration.ResourceGroupName,
		StorageAccountName:       configuration.StorageAccountName,
		SkuName:                  storage.StandardRAGRS,
		Location:                 locationWestUS,
		UseHTTPS:                 true,
		CustomDomainName:         "",
		UseSubDomain:             false,
		EnableEncryption:         false,
		IsCreatedStorageAccount:  false,
		accessKey:                "",
		baseURL:                  "",
		storageManagementClient:  nil,
		storageFileServiceClient: nil,
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

	if err := storageAccount.initManagementClient(); err != nil {
		return nil, err
	}

	return &storageAccount, nil
}

func (account *StorageAccount) initManagementClient() error {
	logger := account.logger.Session("init-management-client")
	logger.Info("start")
	defer logger.Info("end")

	oauthConfig, err := adal.NewOAuthConfig(Environments[account.Environment].ActiveDirectoryEndpointURL, account.TenanID)
	if err != nil {
		logger.Error("newO-auth-config", err, lager.Data{
			"Environment":                account.Environment,
			"ActiveDirectoryEndpointURL": Environments[account.Environment].ActiveDirectoryEndpointURL,
			"TenanID":                    account.TenanID,
		})
		return fmt.Errorf("Error in initManagementClient: %v", err)
	}

	resourceManagerEndpointURL := Environments[account.Environment].ResourceManagerEndpointURL
	spt, err := adal.NewServicePrincipalToken(*oauthConfig, account.ClientID, account.ClientSecret, resourceManagerEndpointURL)
	if err != nil {
		logger.Error("newO-service-principal-token", err, lager.Data{
			"Environment":                account.Environment,
			"resourceManagerEndpointURL": resourceManagerEndpointURL,
			"TenanID":                    account.TenanID,
			"ClientID":                   account.ClientID,
		})
		return fmt.Errorf("Error in initManagementClient: %v", err)
	}

	client := storage.NewAccountsClientWithBaseURI(resourceManagerEndpointURL, account.SubscriptionID)
	account.storageManagementClient = &client
	account.storageManagementClient.Authorizer = autorest.NewBearerAuthorizer(spt)
	return nil
}

func (account *StorageAccount) Exists() (bool, error) {
	logger := account.logger.Session("exists")
	logger.Info("start")
	defer logger.Info("end")

	if _, err := account.GetBaseURL(); err != nil {
		if strings.Contains(err.Error(), resourceNotFound) {
			err = nil
		}
		return false, err
	}
	return true, nil
}

func (account *StorageAccount) GetBaseURL() (string, error) {
	logger := account.logger.Session("get-base-url")
	logger.Info("start")
	defer logger.Info("end")

	if account.baseURL == "" {
		result, err := account.getStorageAccountProperties()
		if err != nil {
			logger.Error("get-storage-account-properties", err)
			return "", err
		}
		account.baseURL, err = parseBaseURL(*result.AccountProperties.PrimaryEndpoints.File)
		if err != nil {
			logger.Error("parse-base-url", err)
			return "", err
		}
	}

	return account.baseURL, nil
}

func (account *StorageAccount) getStorageAccountProperties() (storage.Account, error) {
	logger := account.logger.Session("getStorageAccountProperties").WithData(lager.Data{"StorageAccountName": account.StorageAccountName})
	logger.Info("start")
	defer logger.Info("end")

	result, err := account.storageManagementClient.GetProperties(account.ResourceGroupName, account.StorageAccountName)
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

func (account *StorageAccount) Create() error {
	logger := account.logger.Session("Create")
	logger.Info("start")
	defer logger.Info("end")

	cancel := make(chan struct{})
	sku := storage.Sku{
		Name: account.SkuName,
		Tier: storage.Standard,
	}
	tags := map[string]*string{creator: &userAgent}
	encryptionService := storage.EncryptionService{
		Enabled: &account.EnableEncryption,
	}
	encryption := storage.Encryption{
		Services: &storage.EncryptionServices{
			File: &encryptionService,
			Blob: &encryptionService,
		},
		KeySource: &encryptionKeySource,
	}
	customDomain := storage.CustomDomain{
		Name:         &account.CustomDomainName,
		UseSubDomain: &account.UseSubDomain,
	}
	properties := storage.AccountPropertiesCreateParameters{
		CustomDomain:           &customDomain,
		Encryption:             &encryption,
		EnableHTTPSTrafficOnly: &account.UseHTTPS,
	}
	parameters := storage.AccountCreateParameters{
		Sku:      &sku,
		Kind:     storage.Storage,
		Location: &account.Location,
		Tags:     &tags,
		AccountPropertiesCreateParameters: &properties,
	}
	_, errchan := account.storageManagementClient.Create(account.ResourceGroupName, account.StorageAccountName, parameters, cancel)
	if err := <-errchan; err != nil {
		logger.Error("create-storage-account", err, lager.Data{
			"ResourceGroupName": account.ResourceGroupName,
			"parameters":        parameters,
		})
		return fmt.Errorf("%v", err)
	}
	return nil
}

func (account *StorageAccount) Delete() error {
	logger := account.logger.Session("Delete")
	logger.Info("start")
	defer logger.Info("end")

	_, err := account.storageManagementClient.Delete(account.ResourceGroupName, account.StorageAccountName)
	if err != nil {
		logger.Error("delete-storage-account", err, lager.Data{"ResourceGroupName": account.ResourceGroupName})
	}
	return err
}

func (account *StorageAccount) GetAccessKey() (string, error) {
	logger := account.logger.Session("get-access-key")
	logger.Info("start")
	defer logger.Info("end")

	if account.accessKey == "" {
		result, err := account.storageManagementClient.ListKeys(account.ResourceGroupName, account.StorageAccountName)
		if err != nil {
			logger.Error("list-keys", err, lager.Data{"ResourceGroupName": account.ResourceGroupName})
			return "", fmt.Errorf("Failed to list keys: %v", err)
		}
		account.accessKey = *(*result.Keys)[0].Value
	}
	return account.accessKey, nil
}

func (account *StorageAccount) initFileServiceClient() error {
	logger := account.logger.Session("init-file-service-client")
	logger.Info("start")
	defer logger.Info("end")

	if account.storageFileServiceClient != nil {
		return nil
	}

	if account.accessKey == "" {
		if _, err := account.GetAccessKey(); err != nil {
			return err
		}
	}

	if account.baseURL == "" {
		if _, err := account.GetBaseURL(); err != nil {
			return err
		}
	}

	apiVersion := Environments[account.Environment].APIVersions.Storage
	client, err := file.NewClient(account.StorageAccountName, account.accessKey, account.baseURL, apiVersion, account.UseHTTPS)
	if err != nil {
		logger.Error("new-file-client", err, lager.Data{
			"baseURL":    account.baseURL,
			"apiVersion": apiVersion,
			"UseHTTPS":   account.UseHTTPS,
		})
		return err
	}
	account.storageFileServiceClient = &client
	account.storageFileServiceClient.AddToUserAgent(userAgent)
	return nil
}

func (account *StorageAccount) HasFileShare(fileShareName string) (bool, error) {
	logger := account.logger.Session("has-file-share").WithData(lager.Data{"FileShareName": fileShareName})
	logger.Info("start")
	defer logger.Info("end")

	if err := account.initFileServiceClient(); err != nil {
		return false, err
	}
	fileService := account.storageFileServiceClient.GetFileService()
	share := fileService.GetShareReference(fileShareName)
	exists, err := share.Exists()
	if err != nil {
		logger.Error("check-file-share-exists", err)
	}
	return exists, err
}

func (account *StorageAccount) CreateFileShare(fileShareName string) error {
	logger := account.logger.Session("create-file-share").WithData(lager.Data{"FileShareName": fileShareName})
	logger.Info("start")
	defer logger.Info("end")

	if err := account.initFileServiceClient(); err != nil {
		return err
	}
	fileService := account.storageFileServiceClient.GetFileService()
	share := fileService.GetShareReference(fileShareName)
	options := file.FileRequestOptions{Timeout: timeoutInSeconds}
	err := share.Create(&options)
	if err != nil {
		logger.Error("create-file-share", err)
	}
	return err
}

func (account *StorageAccount) DeleteFileShare(fileShareName string) error {
	logger := account.logger.Session("delete-file-share").WithData(lager.Data{"FileShareName": fileShareName})
	logger.Info("start")
	defer logger.Info("end")

	if err := account.initFileServiceClient(); err != nil {
		return err
	}
	fileService := account.storageFileServiceClient.GetFileService()
	share := fileService.GetShareReference(fileShareName)
	options := file.FileRequestOptions{Timeout: timeoutInSeconds}
	err := share.Delete(&options)
	if err != nil {
		logger.Error("delete-file-share", err)
	}
	return err
}

func (account *StorageAccount) GetShareURL(fileShareName string) (string, error) {
	logger := account.logger.Session("get-share-url").WithData(lager.Data{"FileShareName": fileShareName})
	logger.Info("start")
	defer logger.Info("end")

	if account.baseURL == "" {
		if _, err := account.GetBaseURL(); err != nil {
			return "", err
		}
	}

	return fmt.Sprintf("//%s.file.%s/%s", account.StorageAccountName, account.baseURL, fileShareName), nil
}
