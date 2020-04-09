package sources

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/mitchellh/mapstructure"

	"github.com/Pirionfr/lookatch-agent/utils"

	// driver
	_ "github.com/go-sql-driver/mysql"
	log "github.com/sirupsen/logrus"
)

// MysqlQueryType type of source
const MysqlQueryType = "MysqlQuery"

type (
	// MySQLQuery representation of MySQL Query source
	MySQLQuery struct {
		*DBSQLQuery
		config MysqlQueryConfig
	}

	// MysqlQueryConfig representation MySQL Query configuration
	MysqlQueryConfig struct {
		*DBSQLQueryConfig
		Schema  string   `json:"schema"`
		Exclude []string `json:"exclude"`
	}
)

// NewMysqlQuery create a Mysql Query source
func NewMysqlQuery(s *Source) (SourceI, error) {
	gdbcQuery := NewDBSQLQuery(s)

	mysqlQueryConfig := MysqlQueryConfig{}
	err := s.Conf.UnmarshalKey("sources."+s.Name, &mysqlQueryConfig)
	if err != nil {
		return nil, err
	}

	mysqlQueryConfig.DBSQLQueryConfig = &gdbcQuery.Config

	return &MySQLQuery{
		DBSQLQuery: &gdbcQuery,
		config:     mysqlQueryConfig,
	}, nil
}

// Init initialisation of Mysql Query source
func (m *MySQLQuery) Init() {
	//start bi Query Schema
	err := m.Connect("information_schema")
	if err != nil {
		log.WithError(err).Error("Error while querying Schema")
		return
	}
	err = m.QuerySchema()
	if err != nil {
		log.WithError(err).Error("Error while querying Schema")
		return
	}
	log.Debug("Init Done")
}

// GetStatus returns current status of connexion
func (m *MySQLQuery) GetStatus() interface{} {
	return m.DBSQLQuery.GetStatus()
}

// HealthCheck returns true if source is ok
func (m *MySQLQuery) HealthCheck() bool {
	return m.DBSQLQuery.HealthCheck()
}

// Connect connection to database
func (m *MySQLQuery) Connect(schema string) error {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", m.config.User, m.config.Password, m.config.Host, m.config.Port, schema)
	//dsn += "?tls=skip-verify"

	//first check if db is not already established
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return err
	}
	m.db = db

	err = m.db.Ping()
	if err != nil {
		return err
	}
	return nil
}

// Process process an action
func (m *MySQLQuery) Process(action string, params ...interface{}) interface{} {
	switch action {
	case utils.SourceQuery:
		evSQLQuery := &Query{}
		err := mapstructure.Decode(params[0], evSQLQuery)
		if err != nil {
			log.WithError(err).Error("Unable to decode MySQL Query Statement event")
			return err
		}
		return m.Query(evSQLQuery.Query)

	default:
		return errors.New("task not implemented")
	}
}

// QuerySchema extract schema from database
func (m *MySQLQuery) QuerySchema() (err error) {
	excluded := m.config.Exclude
	notin := "'information_schema','mysql','performance_schema','sys'"

	//see which tables are excluded
	for _, dbname := range excluded {
		notin = notin + ",'" + dbname + "'"
	}
	log.Info("exclude:", notin)

	q := "SELECT TABLE_CATALOG ,TABLE_SCHEMA ,TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, " +
		"CASE WHEN IS_NULLABLE = 'YES' THEN true ELSE false END AS IS_NULLABLE, DATA_TYPE, " +
		"CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, COLUMN_TYPE, COLUMN_KEY FROM COLUMNS " +
		"WHERE TABLE_SCHEMA NOT IN (" + notin + ") ORDER BY TABLE_NAME"

	return m.DBSQLQuery.QuerySchema(q)
}

// Query execute query string
func (m *MySQLQuery) Query(query string) error {
	return m.DBSQLQuery.Query("", query)
}

// QueryMeta execute query meta string
func (m *MySQLQuery) QueryMeta(query string) ([]map[string]interface{}, error) {
	return m.DBSQLQuery.QueryMeta(query)
}
