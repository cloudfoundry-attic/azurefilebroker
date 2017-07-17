package azurefilebroker

import (
	"fmt"
	"net/url"

	"crypto/x509"

	"code.cloudfoundry.org/goshims/ioutilshim"
	"code.cloudfoundry.org/goshims/osshim"
	"code.cloudfoundry.org/goshims/sqlshim"
	"code.cloudfoundry.org/lager"
)

type mssqlVariant struct {
	sql                sqlshim.Sql
	ioutil             ioutilshim.Ioutil
	os                 osshim.Os
	dbConnectionString string
	caCert             string
	dbName             string
}

func NewMSSqlVariant(username, password, host, port, dbName, caCert string, timeoutInSeconds int) SqlVariant {
	return NewMSSqlVariantWithShims(username, password, host, port, dbName, caCert, timeoutInSeconds, &sqlshim.SqlShim{}, &ioutilshim.IoutilShim{}, &osshim.OsShim{})
}

func NewMSSqlVariantWithShims(username, password, host, port, dbName, caCert string, timeoutInSeconds int, sql sqlshim.Sql, ioutil ioutilshim.Ioutil, os osshim.Os) SqlVariant {
	query := url.Values{}
	query.Add("connection timeout", fmt.Sprintf("%d", timeoutInSeconds))

	u := &url.URL{
		Scheme:   "sqlserver",
		User:     url.UserPassword(username, password),
		Host:     fmt.Sprintf("%s:%d", host, port),
		Path:     dbName,
		RawQuery: query.Encode(),
	}
	return &mssqlVariant{
		sql:                sql,
		os:                 os,
		ioutil:             ioutil,
		dbConnectionString: u.String(),
		caCert:             caCert,
		dbName:             dbName,
	}
}

func (c *mssqlVariant) Connect(logger lager.Logger) (sqlshim.SqlDB, error) {
	logger = logger.Session("mssql-connection-connect")
	logger.Info("start")
	defer logger.Info("end")

	if c.caCert == "" {
		c.dbConnectionString = fmt.Sprintf("sqlserver://%s?database=%s", c.dbConnectionString, c.dbName)
	} else {
		certBytes := []byte(c.caCert)

		caCertPool := x509.NewCertPool()
		if ok := caCertPool.AppendCertsFromPEM(certBytes); !ok {
			err := fmt.Errorf("Invalid CA Cert for %s", c.dbName)
			logger.Error("failed-to-parse-sql-ca", err)
			return nil, err
		}

		tmpFile, err := c.ioutil.TempFile("", "mssql-ca-cert")
		if err != nil {
			logger.Error("tempfile-create-failed", err)
			return nil, err
		}

		if _, err := tmpFile.Write([]byte(c.caCert)); err != nil {
			logger.Error("tempfile-write-failed", err)
			return nil, err
		}
		if err := tmpFile.Close(); err != nil {
			logger.Error("tempfile-close-failed", err)
			return nil, err
		}

		c.caCert = tmpFile.Name()
		c.dbConnectionString = fmt.Sprintf("%s?sslmode=verify-ca&sslrootcert=%s", c.dbConnectionString, c.caCert)
	}

	sqlDB, err := c.sql.Open("mssql", c.dbConnectionString)
	return sqlDB, err
}

func (c *mssqlVariant) Flavorify(query string) string {
	return query
}

func (c *mssqlVariant) Close() error {
	if c.caCert != "" {
		return c.os.Remove(c.caCert)
	}
	return nil
}

func (c *mssqlVariant) GetCreateTablesSQL() []string {
	return []string{
		`IF NOT EXISTS (SELECT * from sys.databases WHERE name='service_instances')
		BEGIN
			CREATE TABLE service_instances(
				id VARCHAR(255) PRIMARY KEY,
				value VARCHAR(4096)
			)
		END`,
		`IF NOT EXISTS (SELECT * from sys.databases WHERE name='service_bindings')
		BEGIN
		CREATE TABLE service_bindings(
			id VARCHAR(255) PRIMARY KEY,
			value VARCHAR(4096)
		)
		END`,
		`IF NOT EXISTS (SELECT * from sys.databases WHERE name='file_shares')
		BEGIN
		CREATE TABLE file_shares(
			id VARCHAR(255) PRIMARY KEY,
			instance_id VARCHAR(255),
			FOREIGN KEY (instance_id) REFERENCES service_instances(id),
			file_share_name VARCHAR(255),
			value VARCHAR(4096),
			CONSTRAINT file_share UNIQUE (instance_id, file_share_name)
		)
		END`,
	}
}

func (c *mssqlVariant) GetAppLockSQL() string {
	return "SP_GETAPPLOCK @Resource = ?, @LockTimeout = ?, @LockMode = 'Exclusive', @LockOwner = 'Session'"
}

func (c *mssqlVariant) GetReleaseAppLockSQL() string {
	return "SP_RELEASEAPPLOCK @Resource = ?"
}
