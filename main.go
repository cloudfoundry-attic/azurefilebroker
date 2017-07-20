package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"

	"code.cloudfoundry.org/clock"
	"code.cloudfoundry.org/debugserver"
	"code.cloudfoundry.org/goshims/osshim"
	"code.cloudfoundry.org/lager"
	"code.cloudfoundry.org/lager/lagerflags"
	"github.com/AbelHu/azurefilebroker/azurefilebroker"
	"github.com/AbelHu/azurefilebroker/utils"

	"github.com/pivotal-cf/brokerapi"
	"github.com/tedsuo/ifrit"
	"github.com/tedsuo/ifrit/grouper"
	"github.com/tedsuo/ifrit/http_server"
)

var atAddress = flag.String(
	"listenAddr",
	"0.0.0.0:9000",
	"host:port to serve service broker API",
)

var serviceName = flag.String(
	"serviceName",
	"azuresmbvolume",
	"name of the service to register with cloud controller",
)

var serviceID = flag.String(
	"serviceID",
	"06948cb0-cad7-4buh-leba-9ed8b5c345a3",
	"ID of the service to register with cloud controller",
)

var cfServiceName = flag.String(
	"cfServiceName",
	"",
	"(optional) For CF pushed apps, the service name in VCAP_SERVICES where we should find database credentials. dbDriver must be defined if this option is set, but all other db parameters will be extracted from the service binding.",
)

// DB
var dbDriver = flag.String(
	"dbDriver",
	"",
	"[REQUIRED] - database driver name when using SQL to store broker state",
)

var dbHostname = flag.String(
	"dbHostname",
	"",
	"[REQUIRED] - database hostname when using SQL to store broker state",
)

var dbPort = flag.String(
	"dbPort",
	"",
	"[REQUIRED] - database port when using SQL to store broker state",
)

var dbName = flag.String(
	"dbName",
	"",
	"[REQUIRED] - database name when using SQL to store broker state",
)

var dbCACert = flag.String(
	"dbCACert",
	"",
	"(optional) CA Cert to verify SSL connection. For Azure SQL service, you can use non-empty value to enable TLS encryption",
)

// Bind
var allowedOptions = flag.String(
	"allowedOptions",
	"share,uid,gid,file_mode,dir_mode,readonly,vers,mount",
	"A comma separated list of parameters allowed to be set in during bind operations.",
)

var defaultOptions = flag.String(
	"defaultOptions",
	"vers:3.0",
	"A comma separated list of defaults specified as param:value. If a parameter has a default value and is not in the allowed list, this default value becomes a fixed value that cannot be overridden",
)

// Azure
var environment = flag.String(
	"environment",
	"AzureCloud",
	"The environment for Azure Management Service. AzureCloud, AzureChinaCloud, AzureUSGovernment, AzureGermanCloud or AzureStack.",
)

var tenantID = flag.String(
	"tenantID",
	"",
	"[REQUIRED] - The tenant id for your service principal.",
)

var clientID = flag.String(
	"clientID",
	"",
	"[REQUIRED] - The client id for your service principal.",
)

var clientSecret = flag.String(
	"clientSecret",
	"",
	"[REQUIRED] - The client secret for your service principal.",
)

var defaultSubscriptionID = flag.String(
	"defaultSubscriptionID",
	"",
	"(optional) - The default Azure Subscription id to use when creating new storage accounts.",
)

var defaultResourceGroupName = flag.String(
	"defaultResourceGroupName",
	"",
	"(optional) - The default resource group name to use when creating new storage accounts.",
)

var allowCreateStorageAccount = flag.Bool(
	"allowCreateStorageAccount",
	true,
	"(optional) Allow Broker to create storage accounts.",
)

var allowCreateFileShare = flag.Bool(
	"allowCreateFileShare",
	true,
	"(optional) Allow Broker to create file shares.",
)

var allowDeleteStorageAccount = flag.Bool(
	"allowDeleteStorageAccount",
	false,
	"(optional) Allow Broker to delete storage accounts which are created by Broker.",
)

var allowDeleteFileShare = flag.Bool(
	"allowDeleteFileShare",
	false,
	"(optional) Allow Broker to delete file shares which are created by Broker.",
)

// AzureStack
var azureStackDomain = flag.String(
	"azureStackDomain",
	"",
	"Required when environment is AzureStack. The domain for your AzureStack deployment.",
)

var azureStackAuthentication = flag.String(
	"azureStackAuthentication",
	"",
	"Required when environment is AzureStack. The authentication type for your AzureStack deployment. AzureAD, AzureStackAD or AzureStack.",
)

var azureStackResource = flag.String(
	"azureStackResource",
	"",
	"Required when environment is AzureStack. The token resource for your AzureStack deployment.",
)

var azureStackEndpointPrefix = flag.String(
	"azureStackEndpointPrefix",
	"",
	"Required when environment is AzureStack. The endpoint prefix for your AzureStack deployment.",
)

var (
	username   string
	password   string
	dbUsername string
	dbPassword string
)

func main() {
	parseCommandLine()
	parseEnvironment()

	checkParams()

	sink, err := lager.NewRedactingWriterSink(os.Stdout, lager.INFO, nil, nil)
	if err != nil {
		panic(err)
	}
	logger, logSink := lagerflags.NewFromSink("azurefilebroker", sink)
	logger.Info("starting")
	defer logger.Info("end")

	server := createServer(logger)

	if dbgAddr := debugserver.DebugAddress(flag.CommandLine); dbgAddr != "" {
		server = utils.ProcessRunnerFor(grouper.Members{
			{"debug-server", debugserver.Runner(dbgAddr, logSink)},
			{"broker-api", server},
		})
	}

	process := ifrit.Invoke(server)
	logger.Info("started")
	utils.UntilTerminated(logger, process)
}

func parseCommandLine() {
	lagerflags.AddFlags(flag.CommandLine)
	debugserver.AddFlags(flag.CommandLine)
	flag.Parse()
}

func parseEnvironment() {
	username, _ = os.LookupEnv("USERNAME")
	password, _ = os.LookupEnv("PASSWORD")
	dbUsername, _ = os.LookupEnv("DB_USERNAME")
	dbPassword, _ = os.LookupEnv("DB_PASSWORD")
}

func checkParams() {
	if *dbDriver == "" {
		fmt.Fprint(os.Stderr, "\nERROR: dbDriver parameter is required.\n\n")
		flag.Usage()
		os.Exit(1)
	}
}

// When the broker is running as a CF application, we use db username and password as broker's credential for cloud controler authentication
func parseVcapServices(logger lager.Logger) {
	// populate db parameters from VCAP_SERVICES and pitch a fit if there isn't one.
	services, hasValue := os.LookupEnv("VCAP_SERVICES")
	if !hasValue {
		logger.Fatal("missing-vcap-services-environment", errors.New("missing VCAP_SERVICES environment"))
	}

	servicesValues := map[string][]interface{}{}
	err := json.Unmarshal([]byte(services), &servicesValues)
	if err != nil {
		logger.Fatal("json-unmarshal", err)
	}

	dbServiceValues, ok := servicesValues[*cfServiceName]
	if !ok {
		logger.Fatal("missing-service-binding", errors.New("VCAP_SERVICES missing specified db service"), lager.Data{"servicesValues": servicesValues})
	}

	dbService := dbServiceValues[0].(map[string]interface{})

	credentials := dbService["credentials"].(map[string]interface{})
	logger.Debug("credentials-parsed", lager.Data{"credentials": credentials})

	dbUsername = credentials["username"].(string)
	dbPassword = credentials["password"].(string)
	*dbHostname = credentials["hostname"].(string)
	*dbPort = fmt.Sprintf("%.0f", credentials["port"].(float64))
	*dbName = credentials["name"].(string)
}

func createServer(logger lager.Logger) ifrit.Runner {
	// if we are CF pushed
	if *cfServiceName != "" {
		parseVcapServices(logger)
	}

	store := azurefilebroker.NewStore(logger, *dbDriver, dbUsername, dbPassword, *dbHostname, *dbPort, *dbName, *dbCACert)

	mount := azurefilebroker.NewAzurefilebrokerMountConfig()
	mount.ReadConf(*allowedOptions, *defaultOptions)
	logger.Info("createServer.mount", lager.Data{
		"Allowed": mount.Allowed,
		"Forced":  mount.Forced,
		"Options": mount.Options,
	})

	azureConfig := azurefilebroker.NewAzureConfig(*environment, *tenantID, *clientID, *clientSecret, *defaultSubscriptionID, *defaultResourceGroupName)
	logger.Info("createServer.cloud.azureConfig", lager.Data{
		"Environment":                         azureConfig.Environment,
		"TenanID":                             azureConfig.TenanID,
		"ClientID":                            azureConfig.ClientID,
		"DefaultSubscriptionID":               azureConfig.DefaultSubscriptionID,
		"EnvironDefaultResourceGroupNamement": azureConfig.DefaultResourceGroupName,
	})
	controlConfig := azurefilebroker.NewControlConfig(*allowCreateStorageAccount, *allowCreateFileShare, *allowDeleteStorageAccount, *allowDeleteFileShare)
	logger.Info("createServer.cloud.controlConfig", lager.Data{
		"AllowCreateFileShare":      controlConfig.AllowCreateFileShare,
		"AllowCreateStorageAccount": controlConfig.AllowCreateStorageAccount,
		"AllowDeleteFileShare":      controlConfig.AllowDeleteFileShare,
		"AllowDeleteStorageAccount": controlConfig.AllowDeleteStorageAccount,
	})
	azureStackConfig := azurefilebroker.NewAzureStackConfig(*azureStackDomain, *azureStackAuthentication, *azureStackResource, *azureStackEndpointPrefix)
	logger.Info("createServer.cloud.azureStackConfig", lager.Data{
		"AzureStackAuthentication": azureStackConfig.AzureStackAuthentication,
		"AzureStackDomain":         azureStackConfig.AzureStackDomain,
		"AzureStackEndpointPrefix": azureStackConfig.AzureStackEndpointPrefix,
		"AzureStackResource":       azureStackConfig.AzureStackResource,
	})
	cloud := azurefilebroker.NewAzurefilebrokerCloudConfig(azureConfig, controlConfig, azureStackConfig)

	err := cloud.Validate()
	if err != nil {
		logger.Fatal("createServer.validate-cloud-config", err)
	}

	config := azurefilebroker.NewAzurefilebrokerConfig(mount, cloud)

	serviceBroker := azurefilebroker.New(logger,
		*serviceName, *serviceID,
		&osshim.OsShim{}, clock.NewClock(),
		store, config)

	credentials := brokerapi.BrokerCredentials{Username: username, Password: password}
	handler := brokerapi.New(serviceBroker, logger.Session("broker-api"), credentials)

	return http_server.New(*atAddress, handler)
}
