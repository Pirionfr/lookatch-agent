package sources

import (
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/Pirionfr/lookatch-common/control"

	// driver
	_ "github.com/siddontang/go-mysql/driver"
	log "github.com/sirupsen/logrus"
)

// MysqlQueryType type of source
const MysqlQueryType = "MysqlQuery"

type (
	// MySQLQuery representation of MySQL Query source
	MySQLQuery struct {
		*JDBCQuery
		config MysqlQueryConfig
	}

	// MysqlQueryConfig representation MySQL Query configuration
	MysqlQueryConfig struct {
		*JDBCQueryConfig
		Schema  string   `json:"schema"`
		Exclude []string `json:"exclude"`
	}
)

// newMysqlQuery create a Mysql Query source
func newMysqlQuery(s *Source) (SourceI, error) {
	jdbcQuery := NewJDBCQuery(s)

	mysqlQueryConfig := MysqlQueryConfig{}
	err := s.Conf.UnmarshalKey("sources."+s.Name, &mysqlQueryConfig)
	if err != nil {
		return nil, err
	}

	mysqlQueryConfig.JDBCQueryConfig = &jdbcQuery.Config

	return &MySQLQuery{
		JDBCQuery: &jdbcQuery,
		config:    mysqlQueryConfig,
	}, nil
}

// Init initialisation of Mysql Query source
func (m *MySQLQuery) Init() {

	//start bi Query Schema
	err := m.QuerySchema()
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Error("Error while querying Schema")
		return
	}
	log.Debug("Init Done")
}

// GetStatus returns current status of connexion
func (m *MySQLQuery) GetStatus() interface{} {
	m.Connect("information_schema")
	defer m.db.Close()
	return m.JDBCQuery.GetStatus()
}

// HealthCheck returns true if source is ok
func (m *MySQLQuery) HealthCheck() bool {
	m.Connect("information_schema")
	defer m.db.Close()
	return m.JDBCQuery.HealthCheck()
}

// Connect connection to database
func (m *MySQLQuery) Connect(schema string) {

	dsn := fmt.Sprintf("%s:%s@%s:%d?%s", m.config.User, m.config.Password, m.config.Host, m.config.Port, schema)

	//first check if db is not already established
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Fatal("open mysql connection")
	} else {
		m.db = db
	}

	err = m.db.Ping()
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Error("Connection is dead")
	}

}

// Process process an action
func (m *MySQLQuery) Process(action string, params ...interface{}) interface{} {

	switch action {
	case control.SourceQuery:
		evSQLQuery := &Query{}
		payload := params[0].([]byte)
		err := json.Unmarshal(payload, evSQLQuery)
		if err != nil {
			log.WithFields(log.Fields{
				"error": err,
			}).Fatal("Unable to unmarshal MySQL Query Statement event")
		} else {
			m.Query(evSQLQuery.Query)
		}

	default:
		log.WithFields(log.Fields{
			"action": action,
		}).Error("action not implemented")
	}
	return nil
}

// QuerySchema extract schema from database
func (m *MySQLQuery) QuerySchema() (err error) {

	m.Connect("information_schema")
	defer m.db.Close()

	excluded := m.config.Exclude
	notin := "'information_schema','mysql','performance_schema','sys'"

	//see which tables are excluded
	for _, dbname := range excluded {
		notin = notin + ",'" + dbname + "'"
	}
	log.Info("exclude:", notin)

	q := "SELECT TABLE_CATALOG ,TABLE_SCHEMA ,TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, IS_NULLABLE, DATA_TYPE, " +
		"CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, COLUMN_TYPE, COLUMN_KEY FROM COLUMNS " +
		"WHERE TABLE_SCHEMA NOT IN (" + notin + ") ORDER BY TABLE_NAME"

	m.JDBCQuery.QuerySchema(q)

	return
}

// Query execute query string
func (m *MySQLQuery) Query(query string) {
	m.Connect("information_schema")
	defer m.db.Close()
	m.JDBCQuery.Query("", query)
}

// QueryMeta execute query meta string
func (m *MySQLQuery) QueryMeta(query string) []map[string]interface{} {
	m.Connect("information_schema")
	defer m.db.Close()
	return m.JDBCQuery.QueryMeta(query)

}
