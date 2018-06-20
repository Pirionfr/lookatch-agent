package sources

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/Pirionfr/lookatch-common/control"
	_ "github.com/denisenkom/go-mssqldb"
	log "github.com/sirupsen/logrus"
)

const MSSQLQueryType = "MSSQL"

type (
	MSSQLQuery struct {
		*JDBCQuery
		config MSSQLQueryConfig
	}

	MSSQLQueryConfig struct {
		*JDBCQueryConfig
		SslMode  string `json:"sslmode"`
		Database string `json:"database"`
	}
)

func newMSSQLQuery(s *Source) (SourceI, error) {
	jdbcQuery := NewJDBCQuery(s)

	pgQueryConfig := MSSQLQueryConfig{}
	s.Conf.UnmarshalKey("sources."+s.Name, &pgQueryConfig)
	pgQueryConfig.JDBCQueryConfig = &jdbcQuery.Config

	return &MSSQLQuery{
		JDBCQuery: &jdbcQuery,
		config:    pgQueryConfig,
	}, nil
}

func (m *MSSQLQuery) Init() {

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

func (m *MSSQLQuery) GetStatus() interface{} {
	m.Connect()
	defer m.db.Close()
	return m.JDBCQuery.GetStatus()
}

func (m *MSSQLQuery) HealthCheck() bool {
	m.Connect()
	defer m.db.Close()
	return m.JDBCQuery.HealthCheck()
}

func (m *MSSQLQuery) Connect() {

	dsn := fmt.Sprintf("server=%s;user id=%s;password=%s;port=%d;database=%s",
		m.config.Host,
		m.config.User,
		m.config.Password,
		m.config.Port,
		m.config.Database)
	db, err := sql.Open("sqlserver", dsn)
	//first check if db is not already established
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

func (m *MSSQLQuery) Process(action string, params ...interface{}) interface{} {

	switch action {
	case control.SourceQuery:
		evSqlQuery := &Query{}
		payload := params[0].([]byte)
		err := json.Unmarshal(payload, evSqlQuery)
		if err != nil {
			log.Fatal("Unable to unmarshal MySQL Query Statement event :", err)
		} else {
			m.Query(evSqlQuery.Query)
		}
		break
	default:
		log.WithFields(log.Fields{
			"action": action,
		}).Error("action not implemented")
	}
	return nil
}

func (m *MSSQLQuery) QuerySchema() (err error) {

	m.Connect()
	defer m.db.Close()

	q := `select
		C.TABLE_CATALOG,
		C.table_schema,
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
		from INFORMATION_SCHEMA.COLUMNS C`

	m.JDBCQuery.QuerySchema(q)

	return
}

func (m *MSSQLQuery) Query(query string) {
	m.Connect()
	defer m.db.Close()
	m.JDBCQuery.Query(m.config.Database, query)
}

func (m *MSSQLQuery) QueryMeta(query string, table string, db string, mapAdd map[string]interface{}) map[string]interface{} {
	m.Connect()
	defer m.db.Close()
	return m.JDBCQuery.QueryMeta(query, table, db, mapAdd)
}
