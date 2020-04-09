package sources

import (
	"bytes"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Pirionfr/structs"

	"github.com/Pirionfr/lookatch-agent/events"
	"github.com/Pirionfr/lookatch-agent/utils"

	// driver
	_ "github.com/denisenkom/go-mssqldb"
	log "github.com/sirupsen/logrus"
)

// SqlserverCDCType type of source
const SqlserverCDCType = "SqlserverCDC"

type (
	// SqlserverCDC representation of Sqlserver Query source
	SqlserverCDC struct {
		*Source
		query       *SqlserverQuery
		config      SqlserverCDCConfig
		filter      *utils.Filter
		meta        SqlserverCDCMeta
		db          *sql.DB
		changeTable atomic.Value
	}

	// SqlserverCDCConfig representation Sqlserver Query configuration
	SqlserverCDCConfig struct {
		Host         string                 `json:"host"`
		Port         int                    `json:"port"`
		User         string                 `json:"user"`
		Password     string                 `json:"password"`
		SslMode      string                 `json:"sslmode"`
		Database     string                 `json:"database"`
		PollInterval string                 `json:"poll_interval" mapstructure:"poll_interval"`
		FilterPolicy string                 `json:"filter_policy" mapstructure:"filter_policy"`
		Filter       map[string]interface{} `json:"filter"`
		Enabled      bool                   `json:"enabled"`
		Lsn          string                 `json:"lsn"`
	}

	//SqlserverCDCMeta representation of matadata
	SqlserverCDCMeta struct {
		CurrentLsn string `json:"current_lsn"`
	}
)

// NewSqlserverCDC create a Sqlserver Query source
func NewSqlserverCDC(s *Source) (SourceI, error) {
	MSSqlCDCConfig := SqlserverCDCConfig{}
	err := s.Conf.UnmarshalKey("sources."+s.Name, &MSSqlCDCConfig)
	if err != nil {
		return nil, err
	}

	query := &SqlserverQuery{
		DBSQLQuery: &DBSQLQuery{
			Source: s,
		},
		config: SqlserverQueryConfig{
			DBSQLQueryConfig: &DBSQLQueryConfig{
				Host:     MSSqlCDCConfig.Host,
				Port:     MSSqlCDCConfig.Port,
				User:     MSSqlCDCConfig.User,
				Password: MSSqlCDCConfig.Password,
				NbWorker: 1,
			},
			Database: MSSqlCDCConfig.Database,
		},
	}

	m := &SqlserverCDC{
		Source: s,
		query:  query,
		config: MSSqlCDCConfig,
		filter: &utils.Filter{
			FilterPolicy: MSSqlCDCConfig.FilterPolicy,
			Filter:       MSSqlCDCConfig.Filter,
		},
		meta: SqlserverCDCMeta{},
	}
	m.meta.CurrentLsn = hex.EncodeToString(make([]byte, 10))

	return m, nil
}

// Init initialisation of Sqlserver Query source
func (s *SqlserverCDC) Init() {
	//start bi Query Schema
	err := s.query.QuerySchema()
	if err != nil {
		log.WithError(err).Error("Error while querying Schema")
		return
	}
	log.Debug("Init Done")
}

// Stop source
func (s *SqlserverCDC) Stop() error {
	return nil
}

// Start source
func (s *SqlserverCDC) Start(i ...interface{}) (err error) {
	log.WithFields(log.Fields{
		"Name": s.Name,
		"type": SqlserverCDCType,
	}).Debug("Start")

	err = s.Source.Start(i)
	if err != nil {
		return
	}

	s.Connect()

	s.changeTable.Store(make([]string, 0))
	//Get Next lsn
	currentLsn, err := hex.DecodeString(s.config.Lsn)
	if err == nil {
		s.meta.CurrentLsn = hex.EncodeToString(s.GetNextLsn(currentLsn))
	}

	go s.UpdateChangeTables()

	go s.GetRecordedChances()

	go s.UpdateCommittedLsn()

	return
}

// GetMeta get metadata
func (s *SqlserverCDC) GetMeta() map[string]utils.Meta {
	meta := s.Source.GetMeta()
	if s.Status != SourceStatusWaitingForMETA {
		meta["lsn"] = utils.NewMeta("lsn", s.config.Lsn)
	}

	for k, v := range structs.Map(s.meta) {
		meta[k] = utils.NewMeta(k, v)
	}
	return meta
}

// GetSchema get schema
func (s *SqlserverCDC) GetSchema() map[string]map[string]*Column {
	return s.query.GetSchema()
}

// GetCapabilities returns available actions
func (s *SqlserverCDC) GetCapabilities() map[string]*utils.TaskDescription {
	availableAction := make(map[string]*utils.TaskDescription)
	return availableAction
}

// HealthCheck returns true if source is ok
func (s *SqlserverCDC) HealthCheck() bool {
	if s.db != nil {
		err := s.db.Ping()
		if err != nil || s.db.Stats().OpenConnections == 0 {
			return false
		}
	}
	return true
}

// Connect connection to database
func (s *SqlserverCDC) Connect() {
	dsn := fmt.Sprintf("server=%s;user id=%s;password=%s;port=%d;database=%s",
		s.config.Host,
		s.config.User,
		s.config.Password,
		s.config.Port,
		s.config.Database)
	db, err := sql.Open("sqlserver", dsn)
	//first check if db is not already established
	if err != nil {
		log.WithError(err).Error("Error connecting to Sqlserver source")
		return
	}
	s.db = db

	err = s.db.Ping()
	if err != nil {
		log.WithError(err).Error("Connection is dead")
	}
}

// Process process an action
func (s *SqlserverCDC) Process(action string, params ...interface{}) interface{} {
	switch action {
	case utils.SourceMeta:
		meta := params[0].(map[string]utils.Meta)

		if val, ok := meta["lsn"]; ok {
			s.config.Lsn = val.Value.(string)
		}

		if val, ok := meta["offset_agent"]; ok {
			s.Offset, _ = strconv.ParseInt(val.Value.(string), 10, 64)
		}

		s.Status = SourceStatusRunning

	default:
		return errors.New("task not implemented")
	}
	return nil
}

// GetRecordedChances get cdc change for table list
func (s *SqlserverCDC) GetRecordedChances() {
	poolInterval, _ := time.ParseDuration(s.config.PollInterval)
	ticker := time.NewTicker(poolInterval)
	for range ticker.C {
		err := s.db.Ping()
		if err != nil {
			log.WithError(err).Error("connexion error")
			s.Connect()
			continue
		}

		maxLsn := s.GetMaxLsn()
		currentLsn, _ := hex.DecodeString(s.meta.CurrentLsn)

		if bytes.Compare(currentLsn, maxLsn) >= 0 {
			continue
		}

		//get capture instance
		for _, schemaTable := range s.changeTable.Load().([]string) {
			res := strings.Split(schemaTable, "_")
			schema, table := res[0], res[1]

			pk := s.query.GetPrimary(schema, table)

			rows := s.GetChangesForTables(schemaTable, currentLsn, maxLsn)

			for _, row := range rows {
				switch row["__$operation"].(int64) {
				case 1:
					s.ProcessRow(row, schema, table, pk, "delete")
				case 2:
					s.ProcessRow(row, schema, table, pk, "insert")
				case 4:
					s.ProcessRow(row, schema, table, pk, "update")
				default:
					continue
				}
			}
		}

		s.meta.CurrentLsn = hex.EncodeToString(maxLsn)
	}
}

// ProcessRow send row to chan
func (s *SqlserverCDC) ProcessRow(row map[string]interface{}, schema string, table string, pk string, method string) {
	event := make(map[string]interface{})
	for k, v := range row {
		if !strings.HasPrefix(k, "__$") {
			event[k] = v
		}
	}
	s.Offset++
	s.OutputChannel <- events.LookatchEvent{
		Header: events.LookatchHeader{
			EventType: SqlserverCDCType,
			Tenant:    s.AgentInfo.Tenant,
		},
		Payload: events.SQLEvent{
			Timestamp:   strconv.FormatInt(s.GetTimestampFromLsn(row["__$start_lsn"].([]byte)), 10),
			Environment: s.AgentInfo.Tenant.Env,
			Database:    s.config.Database,
			Schema:      schema,
			Table:       table,
			Method:      method,
			Statement:   event,
			PrimaryKey:  pk,
			Offset: &events.Offset{
				Source: hex.EncodeToString(row["__$start_lsn"].([]byte)),
				Agent:  strconv.FormatInt(s.Offset, 10),
			},
		},
	}
}

// GetMaxLsn get max lsn of server
func (s *SqlserverCDC) GetMaxLsn() []byte {
	result := s.Query("SELECT sys.fn_cdc_get_max_lsn() as max_lsn")
	return result[0]["max_lsn"].([]byte)
}

// GetTimestampFromLsn get Timestamp associated to lsn
func (s *SqlserverCDC) GetTimestampFromLsn(lsn []byte) int64 {
	result := s.Query(fmt.Sprintf("SELECT sys.fn_cdc_map_lsn_to_time(0x%x) as timeLsn", lsn))

	return result[0]["timeLsn"].(time.Time).UnixNano()
}

// GetMinLsn get start lsn of a table
func (s *SqlserverCDC) GetMinLsn(table string) []byte {
	result := s.Query(fmt.Sprintf("SELECT sys.fn_cdc_get_min_lsn('%s') as min_lsn", table))
	return result[0]["min_lsn"].([]byte)
}

// GetMinLsn get start lsn of a table
func (s *SqlserverCDC) GetNextLsn(lsn []byte) []byte {
	result := s.Query(fmt.Sprintf("SELECT sys.fn_cdc_increment_lsn(0x%x) as next_lsn", lsn))
	return result[0]["next_lsn"].([]byte)
}

// UpdateChangeTables update list of activated cdc tables
func (s *SqlserverCDC) UpdateChangeTables() {
	poolInterval, _ := time.ParseDuration(s.config.PollInterval)
	ticker := time.NewTicker(10 * poolInterval)

	for c := ticker.C; ; <-c {
		results := s.Query("SELECT * FROM cdc.change_tables")
		changeTable := make([]string, 0)
		for _, v := range results {
			changeTable = append(changeTable, v["capture_instance"].(string))
		}
		s.changeTable.Store(changeTable)
	}
}

// GetChangesForTables get all change for table between fromLsn and toLsn
func (s *SqlserverCDC) GetChangesForTables(table string, fromLsn []byte, toLsn []byte) []map[string]interface{} {
	updatedFromLsn := fromLsn
	tableMinLsn := s.GetMinLsn(table)
	if bytes.Compare(tableMinLsn, fromLsn) >= 0 {
		updatedFromLsn = tableMinLsn
	}
	return s.Query(fmt.Sprintf("SELECT * FROM cdc.[fn_cdc_get_all_changes_%s](0x%x, 0x%x, N'all update old')", table, updatedFromLsn, toLsn))
}

// Query execute query
func (s *SqlserverCDC) Query(query string) []map[string]interface{} {
	var result []map[string]interface{}
	err := s.db.Ping()
	if err != nil {
		log.WithError(err).Error("Connection is dead")
		return nil
	}
	rows, err := s.db.Query(query)
	if err != nil {
		log.WithError(err).Error("query failed")
		return nil
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
			return nil
		}

		// Create our map, and retrieve the value for each column from the pointers slice,
		// storing it in the map with the name of the column as the key.
		m := make(map[string]interface{})
		for i, colName := range cols {
			val := columnPointers[i].(*interface{})
			m[colName] = *val
		}

		// Outputs: map[columnName:value columnName2:value2 columnName3:value3 ...]
		result = append(result, m)
	}

	return result
}

// UpdateCommittedLsn  update CommittedLsn
func (s *SqlserverCDC) UpdateCommittedLsn() {
	for committedLsn := range s.CommitChannel {
		s.config.Lsn = committedLsn.(string)
	}
}
