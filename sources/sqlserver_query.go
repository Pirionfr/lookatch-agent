package sources

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/mitchellh/mapstructure"

	"github.com/Pirionfr/lookatch-agent/utils"

	// driver
	_ "github.com/denisenkom/go-mssqldb"
	log "github.com/sirupsen/logrus"
)

// SqlserverQueryType type of source
const SqlserverQueryType = "SqlserverQuery"

type (
	// SqlserverQuery representation of Sqlserver Query source
	SqlserverQuery struct {
		*DBSQLQuery
		config SqlserverQueryConfig
	}

	// SqlserverQueryConfig representation Sqlserver Query configuration
	SqlserverQueryConfig struct {
		*DBSQLQueryConfig
		SslMode  string `json:"sslmode"`
		Database string `json:"database"`
	}
)

// NewSqlserverSQLQuery create a Sqlserver Query source
func NewSqlserverSQLQuery(s *Source) (SourceI, error) {
	gdbcQuery := NewDBSQLQuery(s)

	pgQueryConfig := SqlserverQueryConfig{}
	err := s.Conf.UnmarshalKey("sources."+s.Name, &pgQueryConfig)
	if err != nil {
		return nil, err
	}

	pgQueryConfig.DBSQLQueryConfig = &gdbcQuery.Config

	return &SqlserverQuery{
		DBSQLQuery: &gdbcQuery,
		config:     pgQueryConfig,
	}, nil
}

// Init initialisation of Sqlserver Query source
func (m *SqlserverQuery) Init() {
	//start bi Query Schema
	err := m.QuerySchema()
	if err != nil {
		log.WithError(err).Error("Error while querying Schema")
		return
	}
	log.Debug("Init Done")
}

// GetStatus returns the collector's source status
func (m *SqlserverQuery) GetStatus() interface{} {
	m.Connect()
	defer m.db.Close()
	return m.DBSQLQuery.GetStatus()
}

// HealthCheck returns true if the source is correctly configured and the collector is connected to it
func (m *SqlserverQuery) HealthCheck() bool {
	m.Connect()
	defer m.db.Close()
	return m.DBSQLQuery.HealthCheck()
}

// Connect connection to database
func (m *SqlserverQuery) Connect() {
	dsn := fmt.Sprintf("server=%s;user id=%s;password=%s;port=%d;database=%s",
		m.config.Host,
		m.config.User,
		m.config.Password,
		m.config.Port,
		m.config.Database)
	db, err := sql.Open("sqlserver", dsn)
	//first check if db is not already established
	if err != nil {
		log.WithError(err).Error("Error connecting to Sqlserver source")
		return
	}
	m.db = db

	err = m.db.Ping()
	if err != nil {
		log.WithError(err).Error("Connection is dead")
	}
}

// Process process an action
func (m *SqlserverQuery) Process(action string, params ...interface{}) interface{} {
	switch action {
	case utils.SourceQuery:
		evSQLQuery := &Query{}
		err := mapstructure.Decode(params[0], evSQLQuery)
		if err != nil {
			log.WithError(err).Error("Unable to unmarshal Sqlserver Statement")
			return nil
		}
		return m.Query(evSQLQuery.Query)

	default:
		return errors.New("task not implemented")
	}
}

// QuerySchema extract schema from database
func (m *SqlserverQuery) QuerySchema() (err error) {
	m.Connect()
	defer m.db.Close()

	q := `select
		C.TABLE_CATALOG,
		C.TABLE_SCHEMA,
		C.TABLE_NAME,
		C.COLUMN_NAME,
		C.ORDINAL_POSITION,
		CASE WHEN C.IS_NULLABLE = 'YES' THEN 'true' ELSE 'false' END AS IS_NULLABLE,
		C.DATA_TYPE,
		C.CHARACTER_MAXIMUM_LENGTH,
		C.NUMERIC_PRECISION,
		C.NUMERIC_SCALE,
		C.DATA_TYPE,
		CASE WHEN (
		select CONSTRAINT_NAME
		FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE  KCU
		WHERE C.TABLE_CATALOG = KCU.TABLE_CATALOG
		AND C.table_schema = KCU.table_schema
		AND C.TABLE_NAME = KCU.TABLE_NAME
		AND C.COLUMN_NAME = KCU.COLUMN_NAME
		) IS NOT NULL THEN 'PRI' ELSE '' END AS column_key
		from INFORMATION_SCHEMA.COLUMNS C
		WHERE C.TABLE_SCHEMA not in ('cdc')`

	return m.DBSQLQuery.QuerySchema(q)
}

// Query execute query string
func (m *SqlserverQuery) Query(query string) error {
	m.Connect()
	defer m.db.Close()
	return m.DBSQLQuery.Query(m.config.Database, query)
}

// QueryMeta execute query meta string
func (m *SqlserverQuery) QueryMeta(query string) ([]map[string]interface{}, error) {
	m.Connect()
	defer m.db.Close()
	return m.DBSQLQuery.QueryMeta(query)
}
