package sources

import (
	"database/sql"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/remeh/sizedwaitgroup"
	log "github.com/sirupsen/logrus"

	"github.com/Pirionfr/lookatch-agent/utils"

	"github.com/Pirionfr/lookatch-agent/events"
)

const PRI = "PRI"

type (
	// DBSQLQuery representation of DBSQL query
	DBSQLQuery struct {
		*Source
		Config  DBSQLQueryConfig
		db      *sql.DB
		schemas SQLSchema
	}

	// DBSQLQueryConfig representation of DBSQL query configuration
	DBSQLQueryConfig struct {
		Host             string            `json:"host"`
		Port             int               `json:"port"`
		User             string            `json:"user"`
		Password         string            `json:"password"`
		BatchSize        int               `json:"batch_size" mapstructure:"batch_size"`
		NbWorker         int               `json:"nb_worker"  mapstructure:"nb_worker"`
		ColumnsMetaValue bool              `json:"columns_meta" mapstructure:"columns_meta"`
		DefinedPk        map[string]string `json:"defined_pk" mapstructure:"defined_pk"`
	}

	// Query representation of query action
	Query struct {
		Query string `name:"query" description:"SQL query to execute on the source by the collector" required:"true"`
	}
	// SQLSchema  schema     table     Position
	SQLSchema map[string]map[string]map[string]*Column
)

// NewDBSQLQuery create new DBSQL query client
func NewDBSQLQuery(s *Source) DBSQLQuery {
	gdbcQueryConfig := DBSQLQueryConfig{}
	err := s.Conf.UnmarshalKey("sources."+s.Name, &gdbcQueryConfig)
	if err != nil {
		return DBSQLQuery{}
	}
	return DBSQLQuery{
		Source: s,
		Config: gdbcQueryConfig,
	}
}

// GetSchema returns source schema
func (d *DBSQLQuery) GetSchema() map[string]map[string]*Column {
	schema := make(map[string]map[string]*Column)
	var columns map[string]*Column
	var schemaName, tableName string
	for schemaName = range d.schemas {
		for tableName = range d.schemas[schemaName] {
			columns = make(map[string]*Column)
			for index := range d.schemas[schemaName][tableName] {
				columns[d.schemas[schemaName][tableName][index].Column] = d.schemas[schemaName][tableName][index]
			}
			schema[schemaName+"."+tableName] = columns
		}
	}
	return schema
}

// GetStatus returns the collector's source status
func (d *DBSQLQuery) GetStatus() interface{} {
	if d.HealthCheck() {
		return SourceStatusRunning
	}
	return SourceStatusOnError
}

// HealthCheck returns true if the source is correctly configured and the collector is connected to it
func (d *DBSQLQuery) HealthCheck() bool {
	err := d.db.Ping()
	if err != nil || d.db.Stats().OpenConnections == 0 {
		return false
	}
	return true
}

// GetCapabilities returns the actions the collector can perform on this source
func (d *DBSQLQuery) GetCapabilities() map[string]*utils.TaskDescription {
	availableAction := make(map[string]*utils.TaskDescription)
	availableAction[utils.SourceQuery] = utils.DeclareNewTaskDescription(Query{}, "query Gdbc Source")
	return availableAction
}

// QuerySchema retrieves source's schema directly from the source itself
func (d *DBSQLQuery) QuerySchema(q string) (err error) {
	//check connection
	err = d.db.Ping()
	if err != nil {
		log.WithError(err).Error("Connection is dead")
		return
	}

	rows, err := d.db.Query(q)
	if err != nil {
		log.WithError(err).Error("Error while querying the schema")
		return
	}
	defer rows.Close()
	// We initialize the schema map
	d.schemas = make(SQLSchema)

	previousTableName := ""

	for rows.Next() {
		cs := &Column{}
		err := rows.Scan(
			&cs.Database, &cs.Schema, &cs.Table, &cs.Column, &cs.ColumnOrdPos, &cs.Nullable,
			&cs.DataType, &cs.CharacterMaximumLength, &cs.NumericPrecision, &cs.NumericScale, &cs.ColumnType,
			&cs.ColumnKey)

		if err != nil {
			log.WithError(err).Error("Error Discovering")
			return err
		}

		// Log the table name each time a new table is read from the schema
		if previousTableName != cs.Table {
			log.Info("    Discovering: ", cs.Schema, ".", cs.Table)
			previousTableName = cs.Table
		}

		if _, ok := d.schemas[cs.Schema]; !ok {
			t := make(map[string]map[string]*Column)
			d.schemas[cs.Schema] = t
		}
		if _, ok := d.schemas[cs.Schema][cs.Table]; !ok {
			c := make(map[string]*Column)
			d.schemas[cs.Schema][cs.Table] = c
		}

		// Get Defined Primary Key
		dPk := d.Config.DefinedPk[cs.Schema+"."+cs.Table]

		if dPk != "" && utils.InSlice(strings.Split(dPk, ","), cs.Column) {
			cs.ColumnKey = PRI
		}

		d.schemas[cs.Schema][cs.Table][strconv.Itoa(cs.ColumnOrdPos-1)] = cs
	}

	return nil
}

// Query send a SQL query to the configured source
func (d *DBSQLQuery) Query(database string, query string) (err error) {
	//parse query
	schema, table := d.ExtractDatabaseTable(query)

	log.WithField("query", query).Debug("Start querying")
	// check that the collector is still connected to the database
	err = d.db.Ping()
	if err != nil {
		log.WithError(err).Error("Connection is dead")
		return
	}

	//get BatchSize
	var BatchSize int
	if d.Config.BatchSize != 0 {
		BatchSize = d.Config.BatchSize
	} else {
		BatchSize = 5000
	}

	// retrieve the resultset associated with the query to execute
	rows, err := d.db.Query(query)
	if err != nil {
		log.WithFields(log.Fields{
			"query": query,
			"error": err,
		}).Error("Error when query mysql")
		return
	}
	defer rows.Close()

	// Fill in the metadata for each column found in the schema
	var c map[string]events.ColumnsMeta
	var primaryKey string
	//columns list of resultset
	cols, _ := rows.Columns()

	if d.Config.ColumnsMetaValue {
		c = make(map[string]events.ColumnsMeta)
		for index, element := range cols {
			if ok := d.isPrimary(schema, table, strconv.Itoa(index)); ok && primaryKey == "" {
				primaryKey = d.schemas[schema][table][strconv.Itoa(index)].Column
			}
			c[element] = events.ColumnsMeta{
				Type:     d.schemas[schema][table][strconv.Itoa(index)].ColumnType,
				Position: index + 1,
			}
		}
	}

	// The resultset is split into chunks to ease processing
	lineBuffer := make([][]interface{}, BatchSize)

	//The resultset is processed in workers that need to be spawned.
	//The number of worker is specified in the configuration of the collector.
	marshallChan := make(chan map[string]interface{}, BatchSize*d.Config.NbWorker)
	wg := sizedwaitgroup.New(d.Config.NbWorker)

	//l is a counter for chunk size according to batch size
	l := 0
	for rows.Next() {
		//we need to give pointers to scan method
		columns := make([]interface{}, len(cols))
		columnPointers := make([]interface{}, len(cols))
		for i := range columns {
			columnPointers[i] = &columns[i]
		}

		//if we reached BatchSize we reset buffers and launch a processing routine
		if l == BatchSize {
			//do not forget to inc waitgroup for all lines to be processed
			wg.Add()
			//spawn another worker per bunch of BatchSize lines
			go d.MarshallWorker(marshallChan, database, schema, table, primaryKey, c)
			go d.ProcessLines(cols, lineBuffer, marshallChan, &wg)

			//generate a new buffer to prevent reuse of same data container
			lineBuffer = make([][]interface{}, BatchSize)
			l = 0
		}
		//scan values from resultset
		// Scan the result into the column pointers...
		if err = rows.Scan(columnPointers...); err != nil {
			return
		}
		//store current values in chunk
		lineBuffer[l] = columnPointers

		l++
	}
	//do not forget to process last bunch of lines
	if len(lineBuffer[0]) > 0 {
		//spawn another worker per bunch of BatchSize lines
		wg.Add()
		go d.MarshallWorker(marshallChan, database, schema, table, primaryKey, c)
		go d.ProcessLines(cols, lineBuffer, marshallChan, &wg)
	}
	//now wait for all lines to be processed and sent to channel of marshallers
	wg.Wait()
	//then close channel to alert marshaller that query is finished
	log.Debug("Query Done")
	close(marshallChan)
	return nil
}

// MarshallWorker marshall the query statement into json and send it to the output sink
func (d *DBSQLQuery) MarshallWorker(mapchan chan map[string]interface{}, database string, schema string, table string, key string, columnMeta map[string]events.ColumnsMeta) {
	iterator := 0

	header := events.LookatchHeader{
		EventType: MysqlQueryType,
		Tenant:    d.AgentInfo.Tenant,
	}
	for colmap := range mapchan {
		d.Source.OutputChannel <- events.LookatchEvent{
			Header: header,
			Payload: events.SQLEvent{
				Tenant:      d.AgentInfo.Tenant.ID,
				Environment: d.AgentInfo.Tenant.Env,
				Timestamp:   strconv.FormatInt(time.Now().UnixNano(), 10),
				Method:      "query",
				Database:    database,
				Schema:      schema,
				Table:       table,
				Statement:   colmap,
				ColumnsMeta: columnMeta,
				PrimaryKey:  key,
			},
		}
		iterator++
	}
	log.WithField("processedLines", iterator).Debug("Worker done")
}

// ProcessLines process batches of lines from a resultset to map them before sending them to a marshall goroutine
func (d *DBSQLQuery) ProcessLines(columns []string, lines [][]interface{}, mapchan chan map[string]interface{}, wg *sizedwaitgroup.SizedWaitGroup) {
	log.Debug("PROCESSING")
	var colmap map[string]interface{}
	var err error
	for _, values := range lines {
		colmap = make(map[string]interface{})
		if len(values) == 0 {
			break
		}

		for i, col := range columns {
			switch vr := (*values[i].(*interface{})).(type) {
			case []uint8:
				colmap[col], err = strconv.ParseFloat(string(vr), 64)
				if err != nil {
					colmap[col] = string(vr)
				}
			default:
				colmap[col] = vr
			}

		}
		mapchan <- colmap
	}
	log.Debug("PROCESS DONE")
	//when jobs done notify group
	wg.Done()
}

// QueryMeta execute query metadata
func (d *DBSQLQuery) QueryMeta(query string) ([]map[string]interface{}, error) {
	var result []map[string]interface{}
	err := d.db.Ping()
	if err != nil {
		return result, err
	}
	rows, err := d.db.Query(query)
	if err != nil {
		return result, err
	}

	cols, _ := rows.Columns()

	defer rows.Close()

	for rows.Next() {
		// Create a slice of interface{}'s to represent each column,
		// and a second slice to contain pointers to each item in the columns slice.
		columns := make([]interface{}, len(cols))
		columnPointers := make([]interface{}, len(cols))
		for i := range columns {
			columnPointers[i] = &columns[i]
		}

		// Scan the result into the column pointers...
		if err := rows.Scan(columnPointers...); err != nil {
			return nil, err
		}

		// Create our map, and retrieve the value for each column from the pointers slice,
		// storing it in the map with the name of the column as the key.
		m := make(map[string]interface{})
		for i, colName := range cols {
			val := columnPointers[i].(*interface{})
			m[colName] = *val
			v, ok := (*val).([]byte)
			if ok {
				m[colName] = string(v)
			}
		}

		// Outputs: map[columnName:value columnName2:value2 columnName3:value3 ...]
		result = append(result, m)
	}
	return result, nil
}

// ExtractDatabaseTable Extract the database (or schema, depending on your RDBMS) name and table name from a SQL query
func (d *DBSQLQuery) ExtractDatabaseTable(query string) (string, string) {
	r := regexp.MustCompile(`(?i)from[\s]*([\S]+)[\s]*`)
	result := r.FindStringSubmatch(query)
	if len(result) == 0 {
		return "", ""
	}
	s := strings.Split(result[1], ".")
	if len(s) > 1 {
		return strings.Replace(s[0], "\"", "", -1), strings.Replace(s[1], "\"", "", -1)
	}
	return "", strings.Replace(s[0], "\"", "", -1)
}

// isPrimary check if a column is part of a primary key
func (d *DBSQLQuery) isPrimary(schema, table, columnIDStr string) bool {
	if columnSchema, ok := d.schemas[schema][table][columnIDStr]; ok {
		if columnSchema.ColumnKey == "PRI" {
			return true
		}

	}
	return false
}

// GetPrimary check if columns is primary key
func (d *DBSQLQuery) GetPrimary(schema, table string) string {
	for i := range d.schemas[schema][table] {
		if d.schemas[schema][table][i].ColumnKey == "PRI" {
			return d.schemas[schema][table][i].Column
		}
	}
	return ""
}
