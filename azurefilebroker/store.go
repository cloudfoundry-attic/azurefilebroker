package azurefilebroker

import (
	"code.cloudfoundry.org/goshims/ioutilshim"
	"code.cloudfoundry.org/lager"
	"github.com/pivotal-cf/brokerapi"
)

type DataLock interface {
	GetLockForUpdate(lockName string, seconds int) error
	ReleaseLockForUpdate(lockName string) error
}

//go:generate counterfeiter -o ./azurefilebrokerfakes/fake_store.go src/github.com/AbelHu/azurefilebroker/azurefilebroker/Store Store
type Store interface {
	RetrieveServiceInstance(id string) (ServiceInstance, error)
	RetrieveBindingDetails(id string) (brokerapi.BindDetails, error)

	CreateServiceInstance(id string, instance ServiceInstance) error
	CreateBindingDetails(id string, details brokerapi.BindDetails) error

	UpdateServiceInstance(id string, instance ServiceInstance) error

	DeleteServiceInstance(id string) error
	DeleteBindingDetails(id string) error

	IsServiceInstanceConflict(id string, instance ServiceInstance) bool
	IsBindingConflict(id string, details brokerapi.BindDetails) bool

	Restore(logger lager.Logger) error
	Save(logger lager.Logger) error
	Cleanup() error

	DataLock
}

func NewStore(logger lager.Logger, dbDriver, dbUsername, dbPassword, dbHostname, dbPort, dbName, dbCACert, fileName string) Store {
	if dbDriver != "" {
		store, err := NewSqlStore(logger, dbDriver, dbUsername, dbPassword, dbHostname, dbPort, dbName, dbCACert)
		if err != nil {
			logger.Fatal("create-sql-store", err)
		}
		return store
	} else {
		return NewFileStore(fileName, &ioutilshim.IoutilShim{})
	}
}

// Utility methods for storing bindings with secrets stripped out
const HashKey = "paramsHash"

func isServiceInstanceConflict(s Store, id string, _ ServiceInstance) bool {
	if _, err := s.RetrieveServiceInstance(id); err == nil {
		return true
	}
	return false
}

func isBindingConflict(s Store, id string, _ brokerapi.BindDetails) bool {
	if _, err := s.RetrieveBindingDetails(id); err == nil {
		return true
	}
	return false
}
