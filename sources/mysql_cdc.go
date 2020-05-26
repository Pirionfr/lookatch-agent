package sources

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/Pirionfr/structs"
	mysqlLog "github.com/siddontang/go-log/log"
	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"github.com/siddontang/go-mysql/schema"
	log "github.com/sirupsen/logrus"

	"github.com/Pirionfr/lookatch-agent/events"
	"github.com/Pirionfr/lookatch-agent/utils"
)

// MysqlCDCType type of source
const MysqlCDCType = "MysqlCDC"

const (
	UpdateAction = "update"
	InsertAction = "insert"
	DeleteAction = "delete"

	ModeGTID   = "GTID"
	ModeBinlog = "binlog"

	MariadDB = "mariadb"
	Mysql    = "mysql"
)

type (
	// MysqlCDC representation of Mysql change data capture
	MysqlCDC struct {
		*Source
		config    MysqlCDCConfig
		meta      MysqlCDCMeta
		query     *MySQLQuery
		filter    *utils.Filter
		cdcOffset *MysqlOffset
	}

	// MysqlCDCConfig representation of Mysql change data capture configuration
	MysqlCDCConfig struct {
		Enabled          bool                   `json:"enabled"`
		OldValue         bool                   `json:"old_value" mapstructure:"old_value"`
		ColumnsMetaValue bool                   `json:"columns_meta" mapstructure:"columns_meta"`
		SlaveID          uint32                 `json:"slave_id" mapstructure:"slave_id"`
		Host             string                 `json:"host"`
		Port             int                    `json:"port"`
		User             string                 `json:"user"`
		Password         string                 `json:"password"`
		Offset           string                 `json:"offset"`
		Flavor           string                 `json:"flavor"`
		Mode             string                 `json:"mode"`
		FilterPolicy     string                 `json:"filter_policy" mapstructure:"filter_policy"`
		Filter           map[string]interface{} `json:"filter"`
		DefinedPk        map[string]string      `json:"defined_pk" mapstructure:"defined_pk"`
	}

	//MysqlCDCMeta representation of metadata
	MysqlCDCMeta struct {
		ErrorEventNumber int    `json:"error_event_number"`
		CommittedOffset  string `json:"committed_offset"`
	}

	//MysqlOffset representation of binlog and GTID Offset
	MysqlOffset struct {
		sync.RWMutex
		pos  mysql.Position
		gset mysql.GTIDSet
	}
)

// NewMysqlCdc create new mysql CDC source
func NewMysqlCdc(s *Source) (SourceI, error) {
	mysqlCDCConfig := MysqlCDCConfig{}
	err := s.Conf.UnmarshalKey("sources."+s.Name, &mysqlCDCConfig)
	if err != nil {
		return nil, err
	}
	query := &MySQLQuery{
		DBSQLQuery: &DBSQLQuery{
			Source: s,
		},
		config: MysqlQueryConfig{
			DBSQLQueryConfig: &DBSQLQueryConfig{
				Host:      mysqlCDCConfig.Host,
				Port:      mysqlCDCConfig.Port,
				User:      mysqlCDCConfig.User,
				Password:  mysqlCDCConfig.Password,
				NbWorker:  1,
				DefinedPk: mysqlCDCConfig.DefinedPk,
			},
			Schema: "information_schema",
		},
	}

	m := &MysqlCDC{
		Source: s,
		query:  query,
		config: mysqlCDCConfig,
		filter: &utils.Filter{
			FilterPolicy: mysqlCDCConfig.FilterPolicy,
			Filter:       mysqlCDCConfig.Filter,
		},
		meta:      MysqlCDCMeta{},
		cdcOffset: &MysqlOffset{},
	}
	//default value
	if m.config.Flavor == "" {
		m.config.Flavor = Mysql
	}
	if m.config.Mode == "" {
		m.config.Mode = ModeBinlog
	}

	mysqlLog.SetLevel(mysqlLog.LevelError)

	return m, nil
}

// Init source
func (m *MysqlCDC) Init() {
	m.query.Init()
}

// GetSchema get schema
func (m *MysqlCDC) GetSchema() map[string]map[string]*Column {
	return m.query.GetSchema()
}

// Start source
func (m *MysqlCDC) Start(i ...interface{}) (err error) {
	log.WithFields(log.Fields{
		"Name": m.Name,
		"type": MysqlCDCType,
	}).Debug("Start")

	err = m.Source.Start(i)
	if err != nil {
		return err
	}

	go m.UpdateCommittedLsn()

	if m.meta.CommittedOffset == "" {
		m.meta.CommittedOffset = m.config.Offset
	}

	errParse := m.GetValidOffset(m.config.Mode, m.config.Flavor, m.meta.CommittedOffset)
	if errParse != nil {
		//restart from beginning
		m.config.Offset = ""
	} else {
		m.meta.CommittedOffset = m.cdcOffset.OffsetString(m.config.Mode)
	}

	go func() {
		err := m.StartCanal()
		if err != nil {
			log.WithError(err).Error("replication failed")
		}

	}()

	return err
}

// GetMeta get metadata
func (m *MysqlCDC) GetMeta() map[string]utils.Meta {
	log.WithFields(log.Fields{
		"CommittedOffset": m.meta.CommittedOffset,
		"CurrentOffset":   m.cdcOffset.OffsetString(m.config.Mode),
	}).Debug("offset")
	meta := m.Source.GetMeta()

	for k, v := range structs.Map(m.meta) {
		meta[k] = utils.NewMeta(k, v)
	}

	return meta
}

// Process action
func (m *MysqlCDC) Process(action string, params ...interface{}) interface{} {
	switch action {
	case utils.SourceMeta:
		meta := params[0].(map[string]utils.Meta)
		//TODO define offset
		if val, ok := meta["CommittedOffset"]; ok {
			m.meta.CommittedOffset, _ = val.Value.(string)
		}

		if val, ok := meta["OffsetAgent"]; ok {
			m.Offset, _ = strconv.ParseInt(val.Value.(string), 10, 64)
		}

		m.Status = SourceStatusRunning

	default:
		return errors.New("task not implemented")
	}
	return nil
}

// StartCanal decode  event
func (m *MysqlCDC) StartCanal() error {

	cfg := canal.NewDefaultConfig()
	cfg.Addr = fmt.Sprintf("%s:%d", m.config.Host, m.config.Port)
	cfg.User = m.config.User
	cfg.Password = m.config.Password
	cfg.Flavor = m.config.Flavor
	if m.config.SlaveID != 0 {
		cfg.ServerID = m.config.SlaveID
	}
	cfg.Dump.ExecutionPath = ""

	c, _ := canal.NewCanal(cfg)

	// Register a handler to handle RowsEvent
	c.SetEventHandler(m)

	// setup Pos
	switch {
	case m.meta.CommittedOffset == "":
		return c.Run()
	case m.config.Mode == ModeGTID:
		log.WithFields(
			log.Fields{
				"mode":   ModeGTID,
				"offset": m.cdcOffset.GTIDSet(),
			}).Info("Start replication")
		return c.StartFromGTID(m.cdcOffset.GTIDSet())
	case m.config.Mode == ModeBinlog:
		log.WithFields(
			log.Fields{
				"mode":   ModeBinlog,
				"offset": m.cdcOffset.Position().String(),
			}).Info("Start replication")
		return c.RunFrom(m.cdcOffset.Position())
	default:
		return c.Run()
	}

}

// OnPosSynced Use your own way to sync position. When force is true, sync position immediately.
func (m *MysqlCDC) OnPosSynced(pos mysql.Position, gset mysql.GTIDSet, force bool) error {
	if gset != nil {
		m.cdcOffset.UpdateGTIDSet(gset)
	} else {
		m.cdcOffset.Update(pos)
	}
	if force && m.meta.CommittedOffset == "" {
		m.meta.CommittedOffset = m.cdcOffset.OffsetString(m.config.Mode)
	}

	return nil
}

// OnXID store binlog Position
func (m *MysqlCDC) OnXID(pos mysql.Position) error {
	m.cdcOffset.Update(pos)
	return nil
}

// OnGTID store GTID Position
func (m *MysqlCDC) OnGTID(gset mysql.GTIDSet) error {
	m.cdcOffset.UpdateGTIDSet(gset)
	return nil
}

// OnRotate store binlog Position
func (m *MysqlCDC) OnRotate(e *replication.RotateEvent) error {
	pos := mysql.Position{
		Name: string(e.NextLogName),
		Pos:  uint32(e.Position),
	}
	m.cdcOffset.Update(pos)
	return nil
}

// OnTableChanged
func (m *MysqlCDC) OnTableChanged(schema string, table string) error {
	return nil
}

// OnDDL
func (m *MysqlCDC) OnDDL(nextPos mysql.Position, queryEvent *replication.QueryEvent) error {
	return nil
}

// OnRow send row to multiplexer
func (m *MysqlCDC) OnRow(e *canal.RowsEvent) error {

	if m.filter.IsFilteredTable(e.Table.Schema, e.Table.Name) {
		return nil
	}

	m.cdcOffset.UpdatePos(e.Header.LogPos)

	switch e.Action {
	case canal.InsertAction:
		return m.parseEvent(e)
	case canal.DeleteAction:
		return m.parseEvent(e)
	case canal.UpdateAction:
		return m.parseUpdate(e)
	default:
		return nil
	}
}

// String
func (m *MysqlCDC) String() string {
	return "LookatchEventHandler"
}

// parseEvent parse insert and delete rows
func (m *MysqlCDC) parseEvent(e *canal.RowsEvent) error {
	for _, row := range e.Rows {

		event := make(map[string]interface{})
		columnMeta := make(map[string]events.ColumnsMeta)
		if len(e.Table.Columns) != len(row) {
			log.Warn("column Meta and row meta are different")
		}
		for index, columnValue := range row {
			column := e.Table.Columns[index]
			if len(column.EnumValues) > 0 && columnValue != nil {
				idEnum := int(columnValue.(int64))
				if idEnum > 0 {
					event[column.Name] = column.EnumValues[idEnum-1]
				} else {
					event[column.Name] = ""
				}
			} else {
				event[column.Name] = columnValue
			}
			if m.config.ColumnsMetaValue {
				columnMeta[column.Name] = events.ColumnsMeta{
					Type:     column.RawType,
					Position: index + 1,
				}
			}
		}

		m.sendEvent(e.Header.Timestamp, e.Action, e.Table, event, nil, columnMeta)

	}
	return nil
}

// parseUpdate parse update rows
func (m *MysqlCDC) parseUpdate(e *canal.RowsEvent) error {

	for i := 0; i < len(e.Rows); i += 2 {
		oldRow := e.Rows[i]
		row := e.Rows[i+1]
		event := make(map[string]interface{})
		oldEvent := make(map[string]interface{})
		columnMeta := make(map[string]events.ColumnsMeta)
		if len(e.Table.Columns) != len(row) {
			log.Warn("column Meta and row meta are different")
		}
		for index, columnValue := range row {
			column := e.Table.Columns[index]
			if len(column.EnumValues) > 0 && columnValue != nil {
				idEnum := int(columnValue.(int64))
				if idEnum > 0 {
					event[column.Name] = column.EnumValues[idEnum-1]
				} else {
					event[column.Name] = ""
				}
			} else {
				event[column.Name] = columnValue
			}
			if m.config.ColumnsMetaValue {
				columnMeta[column.Name] = events.ColumnsMeta{
					Type:     column.RawType,
					Position: index + 1,
				}
			}
			if m.config.OldValue {
				if len(column.EnumValues) > 0 && e.Rows[i][index] != nil {
					idEnum := int(oldRow[index].(int64))
					if idEnum > 0 {
						oldEvent[column.Name] = column.EnumValues[idEnum-1]
					} else {
						oldEvent[column.Name] = ""
					}
				} else {
					oldEvent[column.Name] = oldRow[index]
				}
			}

		}

		m.sendEvent(e.Header.Timestamp, e.Action, e.Table, event, oldEvent, columnMeta)
	}
	return nil
}

// sendEvent send event to channel
func (m *MysqlCDC) sendEvent(ts uint32, action string, table *schema.Table, event map[string]interface{}, oldEvent map[string]interface{}, columnMeta map[string]events.ColumnsMeta) {
	primaryKey := make([]string, 0)
	var key string

	for i := range table.PKColumns {
		primaryKey = append(primaryKey, table.Columns[i].Name)
	}

	if len(primaryKey) == 0 {
		key = m.config.DefinedPk[table.Schema+"."+table.Name]
	} else {
		key = strings.Join(primaryKey, ",")
	}

	m.Offset++
	m.OutputChannel <- events.LookatchEvent{
		Header: events.LookatchHeader{
			EventType: MysqlCDCType,
			Tenant:    m.AgentInfo.Tenant,
		},
		Payload: events.SQLEvent{
			Timestamp:    fmt.Sprintf("%d%s", ts, "000000000"),
			Environment:  m.AgentInfo.Tenant.Env,
			Database:     table.Schema,
			Table:        table.Name,
			Method:       action,
			ColumnsMeta:  columnMeta,
			OldStatement: oldEvent,
			Statement:    event,
			PrimaryKey:   key,
			Offset: &events.Offset{
				Source: m.cdcOffset.OffsetString(m.config.Mode),
				Agent:  strconv.FormatInt(m.Offset, 10),
			},
		},
	}

}

// GetValidOffset return a valid offset
func (m *MysqlCDC) GetValidOffset(mode string, flavor string, offset string) error {
	if mode == ModeGTID {
		if flavor == Mysql {
			uuidSet, err := m.GetValidMysqlGTIDFromOffset(offset)
			if err != nil {
				return err
			}
			m.cdcOffset.UpdateGTIDSet(uuidSet)
		} else {
			uuidSet, err := m.GetValidMariaDBGTIDFromOffset(offset)
			if err != nil {
				return err
			}
			m.cdcOffset.UpdateGTIDSet(uuidSet)
		}
	} else {
		pos := m.GetValidBinlogFromOffset(offset)
		m.cdcOffset.Update(pos)
	}
	return nil
}

// ParsePosition parse binlog offset from string
func (m *MysqlCDC) ParsePosition(offset string) (mysql.Position, error) {
	pos := mysql.Position{}

	tabFile := strings.Split(offset, ":")

	if len(tabFile) >= 2 && tabFile[0] != "" && tabFile[1] != "" {
		parsePos, err := strconv.Atoi(tabFile[1])
		if err != nil {
			return pos, errors.New("conversion error")
		}
		pos.Pos = uint32(parsePos)
		pos.Name = tabFile[0]
	} else {
		return pos, errors.New("conversion error")
	}
	return pos, nil
}

// GetValidBinlogFromOffset return a valid binlog offset
// if offset is invalid return first binlog offset
// if offset is greater than master position offset return master position
// else return parse offset
func (m *MysqlCDC) GetValidBinlogFromOffset(offset string) mysql.Position {

	firstPosition, _ := m.GetFirstBinlog()
	LastPosition, _ := m.GetLastBinlog()

	storePosition, err := m.ParsePosition(offset)
	if err != nil {
		return firstPosition
	}

	switch {
	case storePosition.Compare(firstPosition) >= 0 && storePosition.Compare(LastPosition) <= 0:
		return storePosition
	case storePosition.Compare(LastPosition) >= 0:
		return LastPosition
	default:
		return firstPosition
	}

}

// GetValidBinlogFromOffset return a valid Mariadb GTID offset
// if offset is invalid return first GTID offset
// if offset is greater than master position offset return master position
// else return parse GTID offset
func (m *MysqlCDC) GetValidMariaDBGTIDFromOffset(offset string) (mysql.GTIDSet, error) {

	pos, _ := m.GetFirstBinlog()
	firstGTIDSet, _ := m.GetGTIDFromMariaDBPosition(pos)

	uuidSet, err := mysql.ParseMariadbGTIDSet(offset)
	if err != nil {
		return firstGTIDSet, nil
	}

	pos, _ = m.GetLastBinlog()
	LastGTIDSet, _ := m.GetGTIDFromMariaDBPosition(pos)

	switch {
	case uuidSet.Contain(firstGTIDSet) && LastGTIDSet.Contain(uuidSet):
		slaveGTIDSet, err := m.GetMariaDBPosGTID()
		if err == nil {
			uuidSet, _ = mysql.ParseMariadbGTIDSet(fmt.Sprintf("%s,%s", slaveGTIDSet.String(), offset))
		}
		return uuidSet, nil
	case uuidSet.Contain(LastGTIDSet):
		return LastGTIDSet, nil
	default:
		return firstGTIDSet, nil
	}

}

// GetGTIDFromMariaDBPosition return mariadb GTID from binlog position
func (m *MysqlCDC) GetGTIDFromMariaDBPosition(pos mysql.Position) (mysql.GTIDSet, error) {
	result, err := m.query.QueryMeta(fmt.Sprintf(`SELECT BINLOG_GTID_POS("%s", %d) as GTID`, pos.Name, pos.Pos))
	if err != nil {
		return nil, err
	}

	if result[0]["GTID"] != nil {
		return mysql.ParseMariadbGTIDSet(result[0]["GTID"].(string))

	}
	return nil, errors.New("can't parse result")
}

// GetMariaDBPosGTID return mariadb GTIDSet from slave
func (m *MysqlCDC) GetMariaDBPosGTID() (mysql.GTIDSet, error) {
	result, err := m.query.QueryMeta("SELECT @@gtid_binlog_pos")
	if err != nil {
		return nil, err
	}

	if result[0]["@@gtid_binlog_pos"] != nil {
		return mysql.ParseMariadbGTIDSet(result[0]["@@gtid_binlog_pos"].(string))

	}
	return nil, errors.New("can't parse result")
}

// GetValidMysqlGTIDFromOffset return a valid Mariadb GTID offset
// parse string offset and add purge GTIDset if exist
func (m *MysqlCDC) GetValidMysqlGTIDFromOffset(offset string) (mysql.GTIDSet, error) {
	var uuid string
	var pos uint32
	_, err := fmt.Sscanf(offset, "%36s:%d", &uuid, &pos)
	if err != nil {
		return nil, errors.New("malformed offset")
	}
	offset = fmt.Sprintf("%s:%d-%d", uuid, 1, pos)
	uuidSet, _ := mysql.ParseMysqlGTIDSet(offset)
	purge, err := m.query.QueryMeta("SELECT @@gtid_purged")
	if err != nil {
		return nil, err
	}
	if len(purge) > 0 {
		gtidPurge, _ := mysql.ParseMysqlGTIDSet(purge[0]["@@gtid_purged"].(string))
		uuidSet, _ = mysql.ParseMysqlGTIDSet(fmt.Sprintf("%s,%s", gtidPurge.String(), offset))
	}
	return uuidSet, nil
}

// GetFirstBinlog Get First Binlog offset
func (m *MysqlCDC) GetFirstBinlog() (mysql.Position, error) {
	pos := mysql.Position{}
	result, err := m.query.QueryMeta("SHOW BINLOG EVENTS limit 1")
	if err != nil {
		return pos, err
	}

	if result[0]["Log_name"] != nil && result[0]["Pos"] != nil {
		readPos, _ := strconv.ParseInt(fmt.Sprintf("%v", result[0]["Pos"]), 10, 32)
		pos.Name = result[0]["Log_name"].(string)
		pos.Pos = uint32(readPos)
		return pos, nil
	}
	return pos, errors.New("can't parse result")
}

// GetLastBinlog Get last Binlog offset
func (m *MysqlCDC) GetLastBinlog() (mysql.Position, error) {
	pos := mysql.Position{}
	result, err := m.query.QueryMeta("SHOW MASTER STATUS")
	if err != nil {
		return pos, err
	}

	if result[0]["File"] != nil && result[0]["Position"] != nil {
		readPos, _ := strconv.ParseInt(fmt.Sprintf("%v", result[0]["Position"]), 10, 32)
		pos.Name = result[0]["File"].(string)
		pos.Pos = uint32(readPos)
		return pos, nil
	}
	return pos, errors.New("can't parse result")
}

// OffsetString convert offset to string
func (m *MysqlOffset) OffsetString(mode string) string {
	if mode == ModeGTID {
		return m.GTIDSet().String()
	}
	return fmt.Sprintf("%s:%d:", m.pos.Name, m.pos.Pos)
}

// Update set pos
func (m *MysqlOffset) Update(pos mysql.Position) {
	m.Lock()
	m.pos = pos
	m.Unlock()
}

// Update set pos
func (m *MysqlOffset) UpdatePos(pos uint32) {
	m.Lock()
	m.pos.Pos = pos
	m.Unlock()
}

// UpdateGTIDSet set GTID pos
func (m *MysqlOffset) UpdateGTIDSet(gset mysql.GTIDSet) {
	m.Lock()
	m.gset = gset
	m.Unlock()
}

// Position get Position
func (m *MysqlOffset) Position() mysql.Position {
	m.RLock()
	defer m.RUnlock()

	return m.pos
}

// GTIDSet get GTID
func (m *MysqlOffset) GTIDSet() mysql.GTIDSet {
	m.RLock()
	defer m.RUnlock()

	if m.gset == nil {
		return nil
	}
	return m.gset.Clone()
}

// UpdateCommittedLsn  update CommittedLsn
func (m *MysqlCDC) UpdateCommittedLsn() {
	for committedOffset := range m.CommitChannel {
		m.meta.CommittedOffset = committedOffset.(string)
	}
}
