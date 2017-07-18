package azurefilebroker

import (
	"fmt"

	"database/sql"

	"encoding/json"

	"code.cloudfoundry.org/lager"
	"github.com/pivotal-cf/brokerapi"
)

type SqlStore struct {
	StoreType string
	Database  SqlConnection
	logger    lager.Logger
}

func NewSqlStore(logger lager.Logger, dbDriver, username, password, host, port, dbName, caCert string) (Store, error) {
	var err error
	var toDatabase SqlVariant
	switch dbDriver {
	case "mysql":
		toDatabase = NewMySqlVariant(username, password, host, port, dbName, caCert)
	case "mssql":
		toDatabase = NewMSSqlVariant(username, password, host, port, dbName, caCert, 30)
	default:
		err = fmt.Errorf("Unrecognized Driver: %s", dbDriver)
		logger.Error("db-driver-unrecognized", err)
		return nil, err
	}
	return NewSqlStoreWithVariant(logger, toDatabase)
}

func NewSqlStoreWithVariant(logger lager.Logger, toDatabase SqlVariant) (Store, error) {
	database := NewSqlConnection(toDatabase)

	err := initialize(logger, database)

	if err != nil {
		logger.Error("sql-store-initialize-database", err)
		return nil, err
	}

	return &SqlStore{
		Database: database,
		logger:   logger.Session("sql-store"),
	}, nil
}

func initialize(logger lager.Logger, db SqlConnection) error {
	logger = logger.Session("initialize-database")
	logger.Info("start")
	defer logger.Info("end")

	var err error
	err = db.Connect(logger)
	if err != nil {
		logger.Error("sql-failed-to-connect", err)
		return err
	}

	for _, query := range db.GetCreateTablesSQL() {
		if _, err := db.Exec(query); err != nil {
			return err
		}
	}
	return nil
}

func (s *SqlStore) Restore() error {
	return nil
}

func (s *SqlStore) Save() error {
	return nil
}

func (s *SqlStore) Cleanup() error {
	return nil
}

func (s *SqlStore) RetrieveServiceInstance(id string) (ServiceInstance, error) {
	var serviceID string
	var value []byte
	var serviceInstance ServiceInstance
	stmt, err := s.Database.Prepare("SELECT id, value FROM service_instances WHERE id = ?")
	if err != nil {
		return ServiceInstance{}, err
	}
	if err := stmt.QueryRow(id).Scan(&serviceID, &value); err == nil {
		err = json.Unmarshal(value, &serviceInstance)
		if err != nil {
			return ServiceInstance{}, err
		}
		return serviceInstance, nil
	} else if err == sql.ErrNoRows {
		return ServiceInstance{}, brokerapi.ErrInstanceDoesNotExist
	}
	return ServiceInstance{}, err
}

func (s *SqlStore) RetrieveBindingDetails(id string) (brokerapi.BindDetails, error) {
	var bindingID string
	var value []byte
	bindDetails := brokerapi.BindDetails{}
	stmt, err := s.Database.Prepare("SELECT id, value FROM service_bindings WHERE id = ?")
	if err != nil {
		return brokerapi.BindDetails{}, err
	}
	if err := stmt.QueryRow(id).Scan(&bindingID, &value); err == nil {
		err = json.Unmarshal(value, &bindDetails)
		if err != nil {
			return brokerapi.BindDetails{}, err
		}
		return bindDetails, nil
	} else if err == sql.ErrNoRows {
		return brokerapi.BindDetails{}, brokerapi.ErrInstanceDoesNotExist
	}
	return brokerapi.BindDetails{}, err
}

func (s *SqlStore) RetrieveFileShare(id string) (FileShare, error) {
	var serviceID string
	var value []byte
	var share FileShare
	stmt, err := s.Database.Prepare("SELECT id, value FROM file_shares WHERE id = ?")
	if err != nil {
		return FileShare{}, err
	}
	if err := stmt.QueryRow(id).Scan(&serviceID, &value); err == nil {
		err = json.Unmarshal(value, &share)
		if err != nil {
			return FileShare{}, err
		}
		return share, nil
	} else if err == sql.ErrNoRows {
		return FileShare{}, brokerapi.ErrInstanceDoesNotExist
	}
	return FileShare{}, err
}

func (s *SqlStore) CreateServiceInstance(id string, instance ServiceInstance) error {
	jsonData, err := json.Marshal(instance)
	if err != nil {
		return err
	}
	stmt, err := s.Database.Prepare("INSERT INTO service_instances (id, organization_guid, space_guid, storage_account_name, value) VALUES (?, ?, ?, ?, ?)")
	if err != nil {
		return err
	}
	_, err = stmt.Exec(id, instance.OrganizationGUID, instance.SpaceGUID, instance.StorageAccountName, jsonData)
	if err != nil {
		return err
	}
	return nil
}

func (s *SqlStore) CreateBindingDetails(id string, details brokerapi.BindDetails) error {
	jsonData, err := json.Marshal(details)
	if err != nil {
		return err
	}
	stmt, err := s.Database.Prepare("INSERT INTO service_bindings (id, value) VALUES (?, ?)")
	if err != nil {
		return err
	}
	_, err = stmt.Exec(id, jsonData)
	if err != nil {
		return err
	}
	return nil
}

func (s *SqlStore) CreateFileShare(id string, share FileShare) error {
	jsonData, err := json.Marshal(share)
	if err != nil {
		return err
	}
	stmt, err := s.Database.Prepare("INSERT INTO file_shares (id, instance_id, file_share_name, value) VALUES (?, ?, ?, ?)")
	if err != nil {
		return err
	}
	_, err = stmt.Exec(id, share.InstanceID, share.FileShareName, jsonData)
	if err != nil {
		return err
	}
	return nil
}

func (s *SqlStore) DeleteServiceInstance(id string) error {
	stmt, err := s.Database.Prepare("DELETE FROM service_instances WHERE id = ?")
	if err != nil {
		return err
	}
	_, err = stmt.Exec(id)
	if err != nil {
		return err
	}
	return nil
}

func (s *SqlStore) DeleteBindingDetails(id string) error {
	stmt, err := s.Database.Prepare("DELETE FROM service_bindings WHERE id = ?")
	if err != nil {
		return err
	}
	_, err = stmt.Exec(id)
	if err != nil {
		return err
	}
	return nil
}

func (s *SqlStore) DeleteFileShare(id string) error {
	stmt, err := s.Database.Prepare("DELETE FROM file_shares WHERE id = ?")
	if err != nil {
		return err
	}
	_, err = stmt.Exec(id)
	if err != nil {
		return err
	}
	return nil
}

func (s *SqlStore) UpdateFileShare(id string, share FileShare) error {
	var serviceID string
	var value []byte
	stmt, err := s.Database.Prepare("SELECT id, value FROM file_shares WHERE id = ?")
	if err != nil {
		return err
	}
	if err := stmt.QueryRow(id).Scan(&serviceID, &value); err != nil {
		if err == sql.ErrNoRows {
			return brokerapi.ErrInstanceDoesNotExist
		}
		return err
	}

	jsonData, err := json.Marshal(share)
	if err != nil {
		return err
	}
	stmt, err = s.Database.Prepare("UPDATE file_shares set value = ? WHERE id = ?")
	if err != nil {
		return err
	}
	_, err = stmt.Exec(jsonData, id)
	if err != nil {
		return err
	}
	return nil
}

func (s *SqlStore) GetLockForUpdate(lockName string, seconds int) error {
	logger := s.logger.WithData(lager.Data{"lockName": lockName, "seconds": seconds})
	query := s.Database.GetAppLockSQL()
	logger.Info("get-lock-for-update", lager.Data{"query": query})
	stmt, err := s.Database.Prepare(query)
	if err != nil {
		logger.Error("prepare-for-get-lock", err)
		return err
	}
	var row string
	if err := stmt.QueryRow(lockName, timeoutInSeconds).Scan(&row); err == nil {
		logger.Info("get-lock-success")
		return nil
	} else if err == sql.ErrNoRows {
		err = fmt.Errorf("Cannot get the lock %q for update in %d seconds", lockName, seconds)
		logger.Error("get-lock-fail", err)
		return err
	}
	logger.Error("get-lock-fail", err)
	return err
}

func (s *SqlStore) ReleaseLockForUpdate(lockName string) error {
	logger := s.logger.WithData(lager.Data{"lockName": lockName})
	query := s.Database.GetReleaseAppLockSQL()
	logger.Info("release-lock-for-update", lager.Data{"query": query})
	stmt, err := s.Database.Prepare(query)
	if err != nil {
		logger.Error("prepare-for-release-lock", err)
		return err
	}
	var row string
	if err := stmt.QueryRow(lockName).Scan(&row); err == nil {
		logger.Info("release-lock-success")
		return nil
	} else if err == sql.ErrNoRows {
		err = fmt.Errorf("Cannot release the lock %q for update", lockName)
		logger.Error("release-lock-fail", err)
		return err
	}
	logger.Error("release-lock-fail", err)
	return err
}
