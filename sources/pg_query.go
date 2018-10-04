package sources

import (
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/Pirionfr/lookatch-common/control"

	// driver
	_ "github.com/lib/pq"
	log "github.com/sirupsen/logrus"
)

// PostgreSQLQueryType type of source
const PostgreSQLQueryType = "postgresqlQuery"

type (
	// PostgreSQLQuery representation of PostgreSQL Query source
	PostgreSQLQuery struct {
		*JDBCQuery
		config PostgreSQLQueryConfig
	}

	//PostgreSQLQueryConfig representation PostgreSQL Query configuration
	PostgreSQLQueryConfig struct {
		*JDBCQueryConfig
		SslMode  string `json:"sslmode"`
		Database string `json:"database"`
	}
)

// newPostgreSQLQuery create a PostgreSQL Query source
func newPostgreSQLQuery(s *Source) (SourceI, error) {
	jdbcQuery := NewJDBCQuery(s)

	pgQueryConfig := PostgreSQLQueryConfig{}
	s.Conf.UnmarshalKey("sources."+s.Name, &pgQueryConfig)
	pgQueryConfig.JDBCQueryConfig = &jdbcQuery.Config

	return &PostgreSQLQuery{
		JDBCQuery: &jdbcQuery,
		config:    pgQueryConfig,
	}, nil
}

// Init initialisation of PostgreSQL Query source
func (p *PostgreSQLQuery) Init() {

	//start bi Query Schema
	err := p.QuerySchema()
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Error("Error while querying Schema")
		return
	}
	log.Debug("Init Done")
}

// GetStatus returns current status of connexion
func (p *PostgreSQLQuery) GetStatus() interface{} {
	p.Connect()
	defer p.db.Close()
	return p.JDBCQuery.GetStatus()
}

// HealthCheck returns true if source is ok
func (p *PostgreSQLQuery) HealthCheck() bool {
	p.Connect()
	defer p.db.Close()
	return p.JDBCQuery.HealthCheck()
}

// Connect connection to database
func (p *PostgreSQLQuery) Connect() {

	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s", p.config.Host, p.config.Port, p.config.User, p.config.Password, p.config.Database, p.config.SslMode)
	db, err := sql.Open("postgres", dsn)
	//first check if db is not already established
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Fatal("open mysql connection")
	} else {
		p.db = db
	}

	err = p.db.Ping()
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Error("Connection is dead")
	}

}

// Process process an action
func (p *PostgreSQLQuery) Process(action string, params ...interface{}) interface{} {

	switch action {
	case control.SourceQuery:
		evSQLQuery := &Query{}
		payload := params[0].([]byte)
		err := json.Unmarshal(payload, evSQLQuery)
		if err != nil {
			log.WithFields(log.Fields{
				"error": err,
			}).Fatal("Unable to unmarshal MySQL Query Statement event :")
		} else {
			p.Query(evSQLQuery.Query)
		}

	default:
		log.WithFields(log.Fields{
			"action": action,
		}).Error("action not implemented")
	}
	return nil
}

// QuerySchema extract schema from database
func (p *PostgreSQLQuery) QuerySchema() (err error) {

	p.Connect()
	defer p.db.Close()

	q := `SELECT
	    c.table_catalog,
	    c.table_schema,
	    c.table_name,
	    c.column_name,
	    c.ordinal_position,
	    CASE WHEN c.is_nullable = 'YES' THEN true ELSE false END AS is_nullable,
	    c.data_type,
	    c.character_maximum_length,
	    c.numeric_precision,
	    c.numeric_scale,
	    format_type(a.atttypid, a.atttypmod) AS column_type,
	    CASE WHEN (
		SELECT  i.indisprimary
		FROM    pg_index i, pg_attribute p
		WHERE   p.attrelid = i.indrelid
		    AND p.attnum = ANY(i.indkey)
		    AND i.indisprimary
		    AND i.indrelid = a.attrelid
		    AND p.attname = c.column_name
		) IS NOT NULL THEN 'PRI' ELSE '' END AS column_key
	FROM    information_schema.columns c,
		pg_attribute a
	WHERE   c.table_schema NOT IN ('information_schema', 'pg_catalog', 'pglogical_origin')
	    AND a.attname = c.column_name
	    AND a.attrelid = (quote_ident(c.table_schema) || '.' || quote_ident(c.table_name))::regclass;`

	p.JDBCQuery.QuerySchema(q)

	return
}

// Query execute query string
func (p *PostgreSQLQuery) Query(query string) {
	p.Connect()
	defer p.db.Close()
	p.JDBCQuery.Query(p.config.Database, query)
}

// QueryMeta execute query meta string
func (p *PostgreSQLQuery) QueryMeta(query string) []map[string]interface{} {
	p.Connect()
	defer p.db.Close()
	return p.JDBCQuery.QueryMeta(query)
}
