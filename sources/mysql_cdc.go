package sources

import (
	"context"
	"encoding/json"
	"fmt"
	utils "github.com/Pirionfr/lookatch-agent/util"
	"github.com/Pirionfr/lookatch-common/control"
	"github.com/Pirionfr/lookatch-common/events"
	"github.com/Pirionfr/lookatch-common/util"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	log "github.com/sirupsen/logrus"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// MysqlCDCType type of source
const MysqlCDCType = "MysqlCDC"

type (
	// MysqlCDC representation of Mysql change data capture
	MysqlCDC struct {
		*Source
		config      MysqlCDCConfig
		query       *MySQLQuery
		filter      *utils.Filter
		syncer      *replication.BinlogSyncer
		logPosition atomic.Value
		logFilename atomic.Value
		status      string
	}

	// MysqlCDCConfig representation of Mysql change data capture configuration
	MysqlCDCConfig struct {
		Host          string                 `json:"host"`
		Port          int                    `json:"port"`
		User          string                 `json:"user"`
		Password      string                 `json:"password"`
		Slave_id      int                    `json:"slave_id"`
		Offset        string                 `json:"offset"`
		LogFile       string                 `json:"logfile"`
		Old_value     bool                   `json:"old_value"`
		Filter_policy string                 `json:"filter_policy"`
		Filter        map[string]interface{} `json:"filter"`
		Enabled       bool                   `json:"enabled"`
	}
)

// newMysqlCdc create new mysql CDC source
func newMysqlCdc(s *Source) (SourceI, error) {
	mysqlCDCConfig := MysqlCDCConfig{}
	s.Conf.UnmarshalKey("sources."+s.Name, &mysqlCDCConfig)

	query := &MySQLQuery{
		JDBCQuery: &JDBCQuery{
			Source: s,
		},
		config: MysqlQueryConfig{
			JDBCQueryConfig: &JDBCQueryConfig{
				Host:     mysqlCDCConfig.Host,
				Port:     mysqlCDCConfig.Port,
				User:     mysqlCDCConfig.User,
				Password: mysqlCDCConfig.Password,
				NbWorker: 1,
			},
			Schema: "information_schema",
		},
	}
	m := &MysqlCDC{
		Source: s,
		query:  query,
		config: mysqlCDCConfig,
		status: control.SourceStatusWaitingForMETA,
		filter: &utils.Filter{
			Filter_policy: mysqlCDCConfig.Filter_policy,
			Filter:        mysqlCDCConfig.Filter,
		},
	}

	m.logFilename.Store("")
	m.logPosition.Store(uint32(0))

	m.readOffset(mysqlCDCConfig.Offset)

	return m, nil
}

// Init source
func (m *MysqlCDC) Init() {

	//start bi Query Schema
	err := m.query.QuerySchema()
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Error("Error while querying Schema")
		return
	}

}

// Stop source
func (m *MysqlCDC) Stop() error {
	return nil
}

// Start source
func (m *MysqlCDC) Start(i ...interface{}) (err error) {
	log.WithFields(log.Fields{
		"type": MysqlCDCType,
	}).Debug("Start")

	cfg := replication.BinlogSyncerConfig{
		ServerID: uint32(m.config.Slave_id),
		Flavor:   "mysql",
		Host:     m.config.Host,
		Port:     uint16(m.config.Port),
		User:     m.config.User,
		Password: m.config.Password,
	}
	m.syncer = replication.NewBinlogSyncer(cfg)

	if !util.IsStandalone(m.Conf) {
		var wg sync.WaitGroup
		wg.Add(1)
		//wait for changeStatus
		go func() {
			for m.status == control.SourceStatusWaitingForMETA {
				time.Sleep(time.Second)
			}
			wg.Done()
		}()
		wg.Wait()
	} else {
		m.status = control.SourceStatusRunning
	}
	m.readValidOffset()

	streamer, err := m.syncer.StartSync(
		mysql.Position{
			Name: m.logFilename.Load().(string),
			Pos:  m.logPosition.Load().(uint32),
		},
	)

	m.decodeBinlog(streamer)
	return
}

// GetName get source source
func (m *MysqlCDC) GetName() string {
	return m.Name
}

// GetOutputChan get output channel
func (m *MysqlCDC) GetOutputChan() chan *events.LookatchEvent {
	return m.OutputChannel
}

// GetMeta get metadata
func (m *MysqlCDC) GetMeta() map[string]interface{} {
	meta := make(map[string]interface{})
	if m.status != control.SourceStatusWaitingForMETA {
		meta["offset"] = m.getOffset()
		meta["offset_agent"] = m.Offset
	}

	return meta
}

// IsEnable check if source is enable
func (m *MysqlCDC) IsEnable() bool {
	return m.config.Enabled
}

// GetSchema get schema
func (m *MysqlCDC) GetSchema() interface{} {
	return m.query.schemas
}

// GetStatus get status
func (m *MysqlCDC) GetStatus() interface{} {
	return m.status
}

// HealthCheck returns true if ok
func (m *MysqlCDC) HealthCheck() bool {
	return m.status == control.SourceStatusRunning
}

// GetAvailableActions returns available actions
func (m *MysqlCDC) GetAvailableActions() map[string]*control.ActionDescription {
	availableAction := make(map[string]*control.ActionDescription)
	return availableAction
}

// Process action
func (m *MysqlCDC) Process(action string, params ...interface{}) interface{} {

	switch action {
	case control.SourceMeta:
		meta := &control.Meta{}
		payload := params[0].([]byte)
		err := json.Unmarshal(payload, meta)
		if err != nil {
			log.WithFields(log.Fields{
				"error": err,
			}).Fatal("Unable to unmarshal MySQL Query Statement event")
		} else {
			if val, ok := meta.Data["offset"].(string); ok {
				m.readOffset(val)
			}

			if val, ok := meta.Data["offset_agent"].(string); ok {
				m.Offset, _ = strconv.ParseInt(val, 10, 64)
			}

			m.status = control.SourceStatusRunning
		}
		break
	default:
		log.WithFields(log.Fields{
			"action": action,
		}).Error("action not implemented")
	}
	return nil
}

// decodeBinlog decode Binlog event
func (m *MysqlCDC) decodeBinlog(streamer *replication.BinlogStreamer) {
	defer func() {
		err := recover()
		if err != nil {
			m.status = control.SourceStatusOnError
			log.WithFields(log.Fields{
				"error": err,
			}).Fatal("MySQLBinlog.Start() has crashed")
		}

	}()
	ctx := context.Background()
	var schema, table string
	var ts int64
	for {

		event, err := streamer.GetEvent(ctx)
		if err != nil {
			m.status = control.SourceStatusOnError
			log.WithFields(log.Fields{
				"error": err,
			}).Error("Error Getting Event")
			continue
		}
		m.status = control.SourceStatusRunning
		if event.Header.LogPos != 0 {
			m.logPosition.Store(event.Header.LogPos)
		}

		switch e := event.Event.(type) {
		case *replication.RowsEvent:
			//log.Debug(e)
			schema = string(e.Table.Schema)
			table = string(e.Table.Table)
			//ts = event.Header.Timestamp
			ts = time.Now().UnixNano()
			if m.filter.IsFilteredTable(schema, table) {
				log.WithFields(log.Fields{
					"schema": schema,
					"table":  table,
				}).Debug("event filtered")
				continue
			}

			switch event.Header.EventType {
			case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
				m.getRows(ts, e.Rows[0], schema, table, "insert")
			case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
				m.getRows(ts, e.Rows[0], schema, table, "delete")
			case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
				if m.config.Old_value {
					m.getRowsWithOldValue(ts, e.Rows, schema, table, "update")
				} else {
					m.getRows(ts, e.Rows[1], schema, table, "update")
				}

			default:
				continue
			}
			break

		case *replication.RotateEvent:
			m.logPosition.Store(uint32(e.Position))
			m.logFilename.Store(string(e.NextLogName))
			break

		}

	}
}

// getRows decode rows
func (m *MysqlCDC) getRows(timestamp int64, row []interface{}, schema, table, method string) {

	//log.Debug("getRows(): event: ",method,schema,table)
	//Columns loop
	colmap := make(map[string]interface{})

	var key string

	for i, col := range row {

		columnIDStr := strconv.Itoa(i)
		columnValue := col

		columnName := ""
		if columnSchema, ok := m.query.schemas[schema][table][columnIDStr]; ok {
			columnName = columnSchema.ColumnName

			if !m.filter.IsFilteredColumn(schema, table, columnName) {
				//Output row number, column number, column type and column value

				colmap[columnName] = columnValue
				if ok := m.query.isPrimary(schema, table, columnIDStr); ok && key == "" {
					key = columnName
				}
			}
		}
	}
	// Serialize
	j, err := json.Marshal(colmap)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Fatal("json.Marshal() error")
	} else {
		m.Offset++
		m.OutputChannel <- &events.LookatchEvent{
			Header: &events.LookatchHeader{
				EventType: MysqlCDCType,
				Tenant:    m.AgentInfo.tenant,
			},
			Payload: &events.SqlEvent{
				Timestamp:   strconv.FormatInt(timestamp, 10),
				Environment: m.AgentInfo.tenant.Env,
				Schema:      schema,
				Table:       table,
				Method:      method,
				Statement:   string(j),
				PrimaryKey:  key,
				Offset: &events.Offset{
					Database: m.getOffset(),
					Agent:    strconv.FormatInt(m.Offset, 10),
				},
			},
		}
	}

}

// getRowsWithOldValue decode old row value
func (m *MysqlCDC) getRowsWithOldValue(timestamp int64, rows [][]interface{}, schema, table, method string) {

	//log.Debug("getRows(): event: ",method,schema,table)
	//Columns loop
	colmap := make(map[string]interface{})
	colmapOld := make(map[string]interface{})
	var key string

	for i, col := range rows[1] {
		columnIDStr := strconv.Itoa(i)
		columnValue := col
		columnValueOld := rows[0][i]
		columnName := string(m.query.schemas[schema][table][strconv.Itoa(i)].ColumnName)

		if !m.filter.IsFilteredColumn(schema, table, columnName) {
			//Output row number, column number, column type and column value
			//println(fmt.Sprintf("%d %d %d %v", i, j, col.GetType(), col.GetValue()))
			//log.Printf("%v=%v", columnName, columnValue)
			colmap[columnName] = columnValue
			colmapOld[columnName] = columnValueOld
			if ok := m.query.isPrimary(schema, table, columnIDStr); ok && key == "" {
				key = columnName
			}

		}
	}
	// Serialize
	j, err := json.Marshal(colmap)
	if err != nil {
		errMsg := "json.Marshal() error"
		log.Panic(errMsg, err)
	}
	k, err := json.Marshal(colmapOld)
	if err != nil {
		errMsg := "json.Marshal() error"
		log.Panic(errMsg, err)
	} else {
		m.Offset++
		m.OutputChannel <- &events.LookatchEvent{
			Header: &events.LookatchHeader{
				EventType: MysqlCDCType,
				Tenant:    m.AgentInfo.tenant,
			},
			Payload: &events.SqlEvent{
				Timestamp:    strconv.Itoa(int(timestamp)),
				Environment:  m.AgentInfo.tenant.Env,
				Database:     schema,
				Table:        table,
				Method:       method,
				Statement:    string(j),
				StatementOld: string(k),
				PrimaryKey:   key,
				Offset: &events.Offset{
					Database: m.getOffset(),
					Agent:    strconv.FormatInt(m.Offset, 10),
				},
			},
		}
	}

}

// GetFirstBinlog Get First Binlog offset
func (m *MysqlCDC) GetFirstBinlog() (string, uint32) {
	m.query.Connect("information_schema")
	defer m.query.db.Close()
	err := m.query.db.Ping()
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Error("Connection is dead")
	}
	q := "SHOW BINLOG EVENTS limit 1"

	result := m.query.QueryMeta(q)
	if result == nil {
		log.Error("Querying first binlog failed")
		return "", 0
	}

	if result[0]["Log_name"] != nil && result[0]["Pos"] != nil {
		pos,_  :=  strconv.ParseInt(result[0]["Pos"].(string), 10, 32)
		return result[0]["Log_name"].(string), uint32(pos)
	}
	return "", 0
}

// GetlastBinlog Get last Binlog offset
func (m *MysqlCDC) GetlastBinlog() (string, uint32) {
	m.query.Connect("information_schema")
	defer m.query.db.Close()
	err := m.query.db.Ping()
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Error("Connection is dead")
	}
	q := "SHOW MASTER STATUS"

	result := m.query.QueryMeta(q)
	if result == nil {
		log.Error("Querying first binlog failed")
		return "", 0
	}

	if result[0]["File"] != nil && result[0]["Position"] != nil {
		pos,_  :=  strconv.ParseInt(result[0]["Position"].(string), 10, 32)
		return result[0]["File"].(string), uint32(pos)
	}
	return "", 0
}

// readValidOffset read Valid Offset
func (m *MysqlCDC) readValidOffset() (err error) {
	//err = m.getOffset()
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Error("read offset error")
	}
	firstFile, firstOffset := m.GetFirstBinlog()
	lastFile, lastOffset := m.GetlastBinlog()

	// ex mysql-db.0005 , 305
	// split mysql file to get increment value
	tabFirstFile := strings.Split(firstFile, ".")
	tabLastFile := strings.Split(lastFile, ".")
	logfile := m.logFilename.Load().(string)
	tabLogfile := strings.Split(logfile, ".")
	pos := m.logPosition.Load().(uint32)
	//check is base filename match
	if tabLogfile[0] == tabFirstFile[0] {
		fileNumCurrent, _ := strconv.Atoi(tabLogfile[1])
		fileNumFirst, _ := strconv.Atoi(tabFirstFile[1])
		fileNumLast, _ := strconv.Atoi(tabLastFile[1])
		//check is filename number between first and last file increment
		if fileNumCurrent >= fileNumFirst && fileNumCurrent <= fileNumLast {
			// check if position is valid
			if fileNumCurrent == fileNumLast {
				if pos <= lastOffset {
					return nil
				}
			} else {
				return nil
			}
		}
	}

	m.logFilename.Store(firstFile)
	pos2 := fmt.Sprint(firstOffset)

	log.WithFields(log.Fields{
		"file":        logfile,
		"position":    pos,
		"firstOffset": firstFile + ":" + pos2,
	}).Debug("Invalid offset restore to first offset")
	m.logPosition.Store(firstOffset)

	return nil
}

//getOffset read offset
func (m *MysqlCDC) getOffset() string {
	position := m.logPosition.Load().(uint32)
	logFilename := m.logFilename.Load().(string)
	m.config.Offset = logFilename + ":" + strconv.Itoa(int(position)) + ":"
	return m.config.Offset
}

// readOffset decode offset
func (m *MysqlCDC) readOffset(offset string) {
	tabFile := strings.Split(offset, ":")

	if tabFile[0] != "" && tabFile[1] != "" {
		pos, err := strconv.Atoi(tabFile[1])
		if err == nil {
			m.logPosition.Store(uint32(pos))

		} else {
			log.WithFields(log.Fields{
				"error": err,
			}).Error("Conversion error")
		}
		m.logFilename.Store(tabFile[0])
	}
}

//get slot status
func (p *PostgreSQLCDC) getSlotStatus() bool {
	// Fetch the restart LSN of the slot, to establish a starting point
	query := fmt.Sprintf("select active from pg_replication_slots where slot_name='%s'", p.config.Slot_name)
	result := p.query.QueryMeta(query)
	if result == nil {
		log.Error("Error while getting Slot Status")
		return false
	}

	if !result[0]["active"].(bool) {
		return false
	}
	return true
}
