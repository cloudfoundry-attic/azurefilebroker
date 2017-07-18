package azurefilebroker

import (
	"code.cloudfoundry.org/goshims/ioutilshim"
	"code.cloudfoundry.org/lager"
	"github.com/pivotal-cf/brokerapi"
)

//go:generate counterfeiter -o ./azurefilebrokerfakes/fake_store.go src/github.com/AbelHu/azurefilebroker/azurefilebroker/Store Store
type Store interface {
	Restore() error
	Save() error
	Cleanup() error

	RetrieveServiceInstance(id string) (ServiceInstance, error)
	RetrieveBindingDetails(id string) (brokerapi.BindDetails, error)
	RetrieveFileShare(id string) (FileShare, error)

	CreateServiceInstance(id string, instance ServiceInstance) error
	CreateBindingDetails(id string, details brokerapi.BindDetails) error
	CreateFileShare(id string, share FileShare) error

	UpdateFileShare(id string, share FileShare) error

	DeleteServiceInstance(id string) error
	DeleteBindingDetails(id string) error
	DeleteFileShare(id string) error

	GetLockForUpdate(lockName string, timeoutInSeconds int) error
	ReleaseLockForUpdate(lockName string) error
}

func NewStore(logger lager.Logger, dbDriver, dbUsername, dbPassword, dbHostname, dbPort, dbName, dbCACert, fileName string) Store {
	if dbDriver != "" {
		store, err := NewSqlStore(logger, dbDriver, dbUsername, dbPassword, dbHostname, dbPort, dbName, dbCACert)
		if err != nil {
			logger.Fatal("create-sql-store", err)
		}
		return store
	}
	return NewFileStore(fileName, &ioutilshim.IoutilShim{}, logger)
}

// Utility methods for storing bindings with secrets stripped out
const HashKey = "paramsHash"
