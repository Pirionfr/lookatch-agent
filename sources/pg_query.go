package sources

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/mitchellh/mapstructure"

	"github.com/Pirionfr/lookatch-agent/utils"

	// driver
	_ "github.com/lib/pq"
	log "github.com/sirupsen/logrus"
)

// PostgreSQLQueryType type of source
const PostgreSQLQueryType = "PostgresqlQuery"

type (
	// PostgreSQLQuery representation of PostgreSQL Query source
	PostgreSQLQuery struct {
		*DBSQLQuery
		config PostgreSQLQueryConfig
	}

	//PostgreSQLQueryConfig representation PostgreSQL Query configuration
	PostgreSQLQueryConfig struct {
		*DBSQLQueryConfig
		SslMode  string `json:"sslmode"`
		Database string `json:"database"`
	}
)

// NewPostgreSQLQuery create a PostgreSQL Query source
func NewPostgreSQLQuery(s *Source) (SourceI, error) {
	gdbcQuery := NewDBSQLQuery(s)

	pgQueryConfig := PostgreSQLQueryConfig{}
	err := s.Conf.UnmarshalKey("sources."+s.Name, &pgQueryConfig)
	if err != nil {
		return nil, err
	}

	pgQueryConfig.DBSQLQueryConfig = &gdbcQuery.Config

	return &PostgreSQLQuery{
		DBSQLQuery: &gdbcQuery,
		config:     pgQueryConfig,
	}, nil
}

// Init initialisation of PostgreSQL Query source
func (p *PostgreSQLQuery) Init() {
	//start bi Query Schema
	p.Connect()
	err := p.QuerySchema()
	if err != nil {
		log.WithError(err).Error("Error while querying Schema")
		return
	}
	log.Debug("Init Done")
}

// GetStatus returns current status of connexion
func (p *PostgreSQLQuery) GetStatus() interface{} {
	return p.DBSQLQuery.GetStatus()
}

// HealthCheck returns true if the source is correctly configured and the collector is connected to it
func (p *PostgreSQLQuery) HealthCheck() bool {
	return p.DBSQLQuery.HealthCheck()
}

// Connect connection to database
func (p *PostgreSQLQuery) Connect() {
	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s", p.config.Host, p.config.Port, p.config.User, p.config.Password, p.config.Database, p.config.SslMode)
	db, err := sql.Open("postgres", dsn)
	//first check if db is not already established
	if err != nil {
		log.WithError(err).Error("open connection")
	} else {
		p.db = db
	}

	err = p.db.Ping()
	if err != nil {
		log.WithError(err).Error("Connection is dead")
	}
}

// Process process an action
func (p *PostgreSQLQuery) Process(action string, params ...interface{}) interface{} {
	switch action {
	case utils.SourceQuery:
		evSQLQuery := &Query{}
		err := mapstructure.Decode(params[0], evSQLQuery)
		if err != nil {
			log.WithError(err).Error("Unable to unmarshal Query Statement event :")
			return err
		}
		return p.Query(evSQLQuery.Query)

	default:
		return errors.New("task not implemented")
	}
}

// QuerySchema extract schema from database
func (p *PostgreSQLQuery) QuerySchema() (err error) {
	err = p.db.Ping()
	if err != nil {
		return err
	}
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

	return p.DBSQLQuery.QuerySchema(q)
}

// Query execute query string
func (p *PostgreSQLQuery) Query(query string) error {
	return p.DBSQLQuery.Query(p.config.Database, query)
}

// QueryMeta execute query meta string
func (p *PostgreSQLQuery) QueryMeta(query string) ([]map[string]interface{}, error) {
	return p.DBSQLQuery.QueryMeta(query)
}
