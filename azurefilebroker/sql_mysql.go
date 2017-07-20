package azurefilebroker

import (
	"fmt"

	"crypto/tls"
	"crypto/x509"
	"time"

	"code.cloudfoundry.org/goshims/sqlshim"
	"code.cloudfoundry.org/lager"
	"github.com/go-sql-driver/mysql"
)

type mysqlVariant struct {
	sql                sqlshim.Sql
	dbConnectionString string
	caCert             string
	dbName             string
	logger             lager.Logger
}

func NewMySqlVariant(logger lager.Logger, username, password, host, port, dbName, caCert string) SqlVariant {
	return NewMySqlVariantWithSqlObject(logger, username, password, host, port, dbName, caCert, &sqlshim.SqlShim{})
}

func NewMySqlVariantWithSqlObject(logger lager.Logger, username, password, host, port, dbName, caCert string, sql sqlshim.Sql) SqlVariant {
	return &mysqlVariant{
		sql:                sql,
		dbConnectionString: fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", username, password, host, port, dbName),
		caCert:             caCert,
		dbName:             dbName,
		logger:             logger,
	}
}

func (c *mysqlVariant) Connect() (sqlshim.SqlDB, error) {
	logger := c.logger.Session("mysql-connection-connect")
	logger.Info("start")
	defer logger.Info("end")

	if c.caCert != "" {
		cfg, err := mysql.ParseDSN(c.dbConnectionString)
		if err != nil {
			logger.Fatal("invalid-db-connection-string", err, lager.Data{"connection-string": c.dbConnectionString})
		}

		logger.Debug("secure-mysql")
		certBytes := []byte(c.caCert)

		caCertPool := x509.NewCertPool()
		if ok := caCertPool.AppendCertsFromPEM(certBytes); !ok {
			err := fmt.Errorf("Invalid CA Cert for %s", c.dbName)
			logger.Error("failed-to-parse-sql-ca", err)
			return nil, err

		}

		tlsConfig := &tls.Config{
			InsecureSkipVerify: false,
			RootCAs:            caCertPool,
		}
		ourKey := "azurefilebroker-tls"
		mysql.RegisterTLSConfig(ourKey, tlsConfig)
		cfg.TLSConfig = ourKey
		cfg.Timeout = 10 * time.Minute
		cfg.ReadTimeout = 10 * time.Minute
		cfg.WriteTimeout = 10 * time.Minute
		c.dbConnectionString = cfg.FormatDSN()
	}

	logger.Info("db-string", lager.Data{"value": c.dbConnectionString})
	sqlDB, err := c.sql.Open("mysql", c.dbConnectionString)
	return sqlDB, err
}

func (c *mysqlVariant) GetInitializeDatabaseSQL() []string {
	return []string{
		`CREATE TABLE IF NOT EXISTS service_instances(
			id VARCHAR(255) PRIMARY KEY,
			organization_guid VARCHAR(255),
			space_guid VARCHAR(255),
			storage_account_name VARCHAR(255),
			value VARCHAR(4096),
			CONSTRAINT storage_account UNIQUE (organization_guid, space_guid, storage_account_name)
		)`,
		`CREATE TABLE IF NOT EXISTS service_bindings(
			id VARCHAR(255) PRIMARY KEY,
			value VARCHAR(4096)
		)`,
		`CREATE TABLE IF NOT EXISTS file_shares(
			id VARCHAR(255) PRIMARY KEY,
			instance_id VARCHAR(255),
			FOREIGN KEY instance_id(instance_id) REFERENCES service_instances(id),
			file_share_name VARCHAR(255),
			value VARCHAR(4096),
			CONSTRAINT file_share UNIQUE (instance_id, file_share_name)
		)`,
	}
}

func (c *mysqlVariant) GetAppLockSQL() string {
	return "SELECT GET_LOCK(?, ?)"
}

func (c *mysqlVariant) GetReleaseAppLockSQL() string {
	return "SELECT RELEASE_LOCK(?)"
}
