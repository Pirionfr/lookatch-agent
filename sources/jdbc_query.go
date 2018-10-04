package sources

import (
	"database/sql"
	"encoding/json"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Pirionfr/lookatch-common/control"
	"github.com/Pirionfr/lookatch-common/events"
	log "github.com/sirupsen/logrus"
)

type (
	// JDBCQuery representation of JDBC query
	JDBCQuery struct {
		*Source
		Config  JDBCQueryConfig
		db      *sql.DB
		schemas SQLSchema
	}

	// JDBCQueryConfig representation of JDBC query configuration
	JDBCQueryConfig struct {
		Host      string `json:"host"`
		Port      int    `json:"port"`
		User      string `json:"user"`
		Password  string `json:"password"`
		BatchSize int    `json:"batchsize"`
		NbWorker  int    `json:"nbworker"`
	}

	// ColumnSchema representation of schema
	ColumnSchema struct {
		TableDatabase          string
		TableSchema            string
		TableName              string
		ColumnName             string
		ColumnOrdPos           int
		IsNullable             string
		DataType               string
		CharacterMaximumLength sql.NullInt64
		NumericPrecision       sql.NullInt64
		NumericScale           sql.NullInt64
		ColumnType             string
		ColumnKey              string
	}
	// Query representation of query action
	Query struct {
		Query string `description:"SQL query to execute on agent" required:"true"`
	}
	// SQLSchema  schema     table     Position
	SQLSchema map[string]map[string]map[string]*ColumnSchema
)

// NewJDBCQuery create new JDBC query client
func NewJDBCQuery(s *Source) JDBCQuery {
	jdbcQueryConfig := JDBCQueryConfig{}
	s.Conf.UnmarshalKey("sources."+s.Name, &jdbcQueryConfig)

	return JDBCQuery{
		Source: s,
		Config: jdbcQueryConfig,
	}
}

// Stop source
func (j *JDBCQuery) Stop() error {
	return nil
}

// Start source
func (j *JDBCQuery) Start(i ...interface{}) (err error) {
	return
}

// GetName get name of source
func (j *JDBCQuery) GetName() string {
	return j.Name
}

//GetOutputChan get output channel
func (j *JDBCQuery) GetOutputChan() chan *events.LookatchEvent {
	return j.OutputChannel
}

// GetMeta returns source meta
func (j *JDBCQuery) GetMeta() map[string]interface{} {
	meta := make(map[string]interface{})
	return meta
}

// IsEnable check if source is enable
func (j *JDBCQuery) IsEnable() bool {
	return true
}

// GetSchema returns schema
func (j *JDBCQuery) GetSchema() interface{} {
	return j.schemas
}

// GetStatus get source status
func (j *JDBCQuery) GetStatus() interface{} {
	if j.HealthCheck() {
		return control.SourceStatusRunning
	}
	return control.SourceStatusOnError
}

// HealthCheck return true if ok
func (j *JDBCQuery) HealthCheck() bool {
	err := j.db.Ping()
	if err != nil || j.db.Stats().OpenConnections == 0 {
		return false
	}
	return true

}

// GetAvailableActions returns available actions
func (j *JDBCQuery) GetAvailableActions() map[string]*control.ActionDescription {
	availableAction := make(map[string]*control.ActionDescription)
	availableAction[control.SourceQuery] = control.DeclareNewAction(Query{}, "query Jdbc Source")
	return availableAction
}

// QuerySchema query schema
func (j *JDBCQuery) QuerySchema(q string) (err error) {
	//check connection
	err = j.db.Ping()
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Error("Connection is dead")
		return
	}

	rows, err := j.db.Query(q)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Error("Error when Query Schema")
		return
	}
	defer rows.Close()
	// We initialize the schema map
	j.schemas = make(SQLSchema)

	previousTableName := ""

	for rows.Next() {
		cs := &ColumnSchema{}
		err := rows.Scan(&cs.TableDatabase, &cs.TableSchema, &cs.TableName, &cs.ColumnName, &cs.ColumnOrdPos, &cs.IsNullable, &cs.DataType,
			&cs.CharacterMaximumLength, &cs.NumericPrecision, &cs.NumericScale, &cs.ColumnType, &cs.ColumnKey)

		if err != nil {
			log.WithFields(log.Fields{
				"error": err,
			}).Error("Error Discovering")
			return err
		}

		//print only 1 time the table name
		if previousTableName != cs.TableName {
			log.Info("    Discovering: ", cs.TableSchema, ".", cs.TableName)
			previousTableName = cs.TableName
		}

		if _, ok := j.schemas[cs.TableSchema]; !ok {
			t := make(map[string]map[string]*ColumnSchema)
			j.schemas[cs.TableSchema] = t
		}
		if _, ok := j.schemas[cs.TableSchema][cs.TableName]; !ok {
			c := make(map[string]*ColumnSchema)
			j.schemas[cs.TableSchema][cs.TableName] = c
		}

		j.schemas[cs.TableSchema][cs.TableName][strconv.Itoa(cs.ColumnOrdPos-1)] = cs

	}

	return
}

//Query execute query
func (j *JDBCQuery) Query(database string, query string) {

	//parse query
	schema, table := j.ExtractDatabaseTable(query)

	//check if database connection is up
	log.WithFields(log.Fields{
		"query": query,
	}).Debug("Stary Query")

	errdb := j.db.Ping()
	if errdb != nil {
		log.WithFields(log.Fields{
			"error": errdb,
		}).Error("Connection is dead")
		return
	}

	//get BatchSize
	var BatchSize int
	if j.Config.BatchSize != 0 {
		BatchSize = j.Config.BatchSize
	} else {
		BatchSize = 5000
	}

	//generate resultset from given query
	rows, err := j.db.Query(query)
	if err != nil {
		log.WithFields(log.Fields{
			"query": query,
			"error": err,
		}).Error("Error when query mysql")
		return
	}

	//columns list of resultset
	columns, _ := rows.Columns()
	//column number in list
	count := len(columns)
	//slice which will contain values of a row
	values := make([]interface{}, count)
	//a pointer to fill values slice
	valuePtrs := make([]interface{}, count)
	//slice of values (slice of rows) to split resultset processing in chuncks
	lineBuffer := make([][]interface{}, BatchSize)

	//we need to give pointers to scan method
	for i := range columns {
		valuePtrs[i] = &values[i]
	}

	//spawn stack of workers if specified in conf (for huge needs)
	//create chan here in order to close goroutine when query is finished
	marshallChan := make(chan map[string]interface{}, 100000)
	wg := sync.WaitGroup{}
	for i := 0; i < j.Config.NbWorker; i++ {
		go j.MarshallWorker(marshallChan, database, schema, table)
	}

	//l is a counter for chunk size according to batch size
	l := 0
	for rows.Next() {
		//if we reached BatchSize we reset buffers and lauch a processing routine
		if l == BatchSize {
			//spawn another worker per bunch of BatchSize lines
			go j.MarshallWorker(marshallChan, database, schema, table)
			//do not forget to inc waitgroup for all lines to be processed
			wg.Add(1)
			go j.ProcessLines(columns, lineBuffer, marshallChan, &wg)

			//generate a new buffer to prevent reuse of same data container
			lineBuffer = make([][]interface{}, BatchSize)
			l = 0
		}
		//scan values from resultset
		rows.Scan(valuePtrs...)
		//store current values in chunk
		lineBuffer[l] = values
		//create a new values slice
		values = make([]interface{}, count)
		for i := range columns {
			valuePtrs[i] = &values[i]
		}
		l++
	}
	//do not forget to process last bunch of lines
	if len(lineBuffer[0]) > 0 {
		//spawn another worker per bunch of BatchSize lines
		go j.MarshallWorker(marshallChan, database, schema, table)
		wg.Add(1)
		go j.ProcessLines(columns, lineBuffer, marshallChan, &wg)
	}
	//now wait for all lines to be processed and sent to channel of marshallers
	wg.Wait()
	//then close channel to alert marshaller that query is finished
	log.Debug("Query Done")
	close(marshallChan)
}

// MarshallWorker marshalling statement into json and sent to output sink
func (j *JDBCQuery) MarshallWorker(mapchan chan map[string]interface{}, database string, schema string, table string) {
	iterator := 0
	for colmap := range mapchan {

		statement, _ := json.Marshal(colmap)

		ev := &events.LookatchEvent{
			Header: &events.LookatchHeader{
				EventType: MysqlQueryType,
				Tenant:    j.AgentInfo.tenant,
			},
			Payload: &events.SqlEvent{
				Tenant:      j.AgentInfo.tenant.Id,
				Environment: j.AgentInfo.tenant.Env,
				Timestamp:   strconv.FormatInt(time.Now().UnixNano(), 10),
				Method:      "query",
				Database:    database,
				Schema:      schema,
				Table:       table,
				Statement:   string(statement),
			},
		}
		j.Source.OutputChannel <- ev
		iterator++
	}
	log.WithFields(log.Fields{
		"processedLines": iterator,
	}).Debug("Worker done")

}

// ProcessLines process bunch of lines  from resultset to map and sent to marshall goroutine
func (j *JDBCQuery) ProcessLines(columns []string, lines [][]interface{}, mapchan chan map[string]interface{}, wg *sync.WaitGroup) {

	log.Debug("PROCESSING")
	for _, values := range lines {
		colmap := make(map[string]interface{})
		if len(values) == 0 {
			break
		}

		for i, col := range columns {
			var v interface{}
			val := values[i]
			b, ok := val.([]byte)
			//cast to string because of particular behaviour in next step
			if ok {
				v = string(b)
			} else {
				v = val
			}
			colmap[col] = v
		}
		mapchan <- colmap
	}
	log.Debug("PROCESS DONE")
	//when jobs done notify group
	wg.Done()
}

// QueryMeta execute query metadata
func (j *JDBCQuery) QueryMeta(query string) []map[string]interface{} {

	var result []map[string]interface{}
	err := j.db.Ping()
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Fatal("Connection is dead")
	}
	rows, err := j.db.Query(query)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Error("query failed")
		return result
	}

	columns, _ := rows.Columns()
	count := len(columns)
	values := make([]interface{}, count)
	valuePtrs := make([]interface{}, count)
	colmap := make(map[string]interface{})

	for rows.Next() {
		for i := range columns {
			valuePtrs[i] = &values[i]
		}
		rows.Scan(valuePtrs...)
		for i, col := range columns {
			var v interface{}
			val := values[i]
			b, ok := val.([]byte)
			if ok {
				v = string(b)
			} else {
				v = val
			}
			colmap[col] = v
		}
		result = append(result, colmap)
	}
	return result
}

// ExtractDatabaseTable Extract Database and Table from query
func (j *JDBCQuery) ExtractDatabaseTable(query string) (string, string) {
	r, _ := regexp.Compile(`(?i)from\s(\S+)(\swhere)?`)
	result := r.FindStringSubmatch(query)
	if len(result) == 0 {
		return "", ""
	}
	s := strings.Split(result[1], ".")
	if len(s) > 1 {
		return s[0], s[1]
	}
	return "", s[0]

}

// isPrimary check if columns is primary key
func (j *JDBCQuery) isPrimary(schema, table, columnIDStr string) bool {
	if columnSchema, ok := j.schemas[schema][table][columnIDStr]; ok {
		if columnSchema.ColumnKey == "PRI" {
			return true
		}
	}
	return false
}
