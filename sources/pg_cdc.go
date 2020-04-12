package sources

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Pirionfr/structs"
	"github.com/jackc/pgconn"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgproto3/v2"
	log "github.com/sirupsen/logrus"

	"github.com/Pirionfr/lookatch-agent/events"
	"github.com/Pirionfr/lookatch-agent/utils"
)

type (
	// PostgreSQLCDC representation of PostgreSQL change data capture
	PostgreSQLCDC struct {
		*Source
		config         PostgreSQLCDCConf
		query          *PostgreSQLQuery
		filter         *utils.Filter
		conn           *pgconn.PgConn
		meta           Meta
		ctx            context.Context
		CommittedState *OffsetCommittedState
	}

	// PostgreSQLCDCConf representation of PostgreSQL change data capture configuration
	PostgreSQLCDCConf struct {
		Enabled          bool                   `json:"enabled"`
		OldValue         bool                   `json:"old_value" mapstructure:"old_value"`
		ColumnsMetaValue bool                   `json:"columns_meta" mapstructure:"columns_meta"`
		Host             string                 `json:"host"`
		Port             int                    `json:"port"`
		User             string                 `json:"user"`
		Password         string                 `json:"password"`
		SslMode          string                 `json:"sslmode"`
		Database         string                 `json:"database"`
		SlotName         string                 `json:"slot_name" mapstructure:"slot_name"`
		FilterPolicy     string                 `json:"filter_policy" mapstructure:"filter_policy"`
		Filter           map[string]interface{} `json:"filter"`
		DefinedPk        map[string]string      `json:"defined_pk" mapstructure:"defined_pk"`
	}

	// Messages representation of messages
	Messages struct {
		Change []Message `json:"change"`
	}

	// Message representation of message
	Message struct {
		Columnnames  []string      `json:"columnnames"`
		Columntypes  []string      `json:"columntypes"`
		Columnvalues []interface{} `json:"columnvalues"`
		Kind         string        `json:"kind"`
		Schema       string        `json:"schema"`
		Table        string        `json:"table"`
		Oldkeys      Oldkeys       `json:"oldkeys"`
	}

	// Oldkeys representation of old event
	Oldkeys struct {
		Keynames  []string      `json:"keynames"`
		Keytypes  []string      `json:"keytypes"`
		Keyvalues []interface{} `json:"keyvalues"`
	}

	// Meta representation of metadata
	Meta struct {
		LastState    string        `json:"laststate"`
		CurrentLsn   pglogrepl.LSN `json:"current_lsn"`
		CommittedLsn pglogrepl.LSN `json:"committed_lsn"`
		SlotStatus   bool          `json:"slotstatus"`
		ServerWALEnd pglogrepl.LSN `json:"werver_wal_end"`
	}

	//OffsetComittedState keep track offSend Event
	OffsetCommittedState struct {
		sync.RWMutex
		SendedLsn []pglogrepl.LSN
	}
)

// PostgreSQLCDCType type of source
const PostgreSQLCDCType = "PostgresqlCDC"

// TickerValue number second to wait between to tick
const TickerValue = 10

// NewPostgreSQLCdc create new PostgreSQL CDC source
func NewPostgreSQLCdc(s *Source) (SourceI, error) {
	postgreSQLCDCConf := PostgreSQLCDCConf{}
	err := s.Conf.UnmarshalKey("sources."+s.Name, &postgreSQLCDCConf)
	if err != nil {
		return nil, err
	}

	query := &PostgreSQLQuery{
		DBSQLQuery: &DBSQLQuery{
			Source: s,
		},
		config: PostgreSQLQueryConfig{
			DBSQLQueryConfig: &DBSQLQueryConfig{
				Host:      postgreSQLCDCConf.Host,
				Port:      postgreSQLCDCConf.Port,
				User:      postgreSQLCDCConf.User,
				Password:  postgreSQLCDCConf.Password,
				NbWorker:  1,
				DefinedPk: postgreSQLCDCConf.DefinedPk,
			},
			Database: postgreSQLCDCConf.Database,
			SslMode:  postgreSQLCDCConf.SslMode,
		},
	}
	p := &PostgreSQLCDC{
		Source: s,
		query:  query,
		config: postgreSQLCDCConf,
		filter: &utils.Filter{
			FilterPolicy: postgreSQLCDCConf.FilterPolicy,
			Filter:       postgreSQLCDCConf.Filter,
		},
		ctx:            context.Background(),
		CommittedState: NewOffsetCommittedState(),
	}

	return p, nil
}

// Init source
func (p *PostgreSQLCDC) Init() {
	p.query.Init()
}

// GetSchema get schema
func (p *PostgreSQLCDC) GetSchema() map[string]map[string]*Column {
	return p.query.GetSchema()
}

// Start source
func (p *PostgreSQLCDC) Start(i ...interface{}) (err error) {
	log.WithField("type", PostgreSQLCDCType).Info("Start")

	err = p.Source.Start(i)
	if err != nil {
		return
	}

	go p.UpdateCommittedLsn()

	p.StartReplication()
	// consume events
	go p.decodeEvents()

	return nil
}

// GetMeta get metadata
func (p *PostgreSQLCDC) GetMeta() map[string]utils.Meta {
	meta := p.Source.GetMeta()

	for k, v := range structs.Map(p.meta) {
		meta[k] = utils.NewMeta(k, v)
	}
	return meta
}

// HealthCheck returns true if ok
func (p *PostgreSQLCDC) HealthCheck() bool {
	return p.Source.HealthCheck() && p.meta.SlotStatus
}

// Process action
func (p *PostgreSQLCDC) Process(action string, params ...interface{}) interface{} {
	switch action {
	case utils.SourceMeta:
		meta := params[0].(map[string]utils.Meta)

		if val, ok := meta["offset_agent"]; ok {
			p.Offset, _ = strconv.ParseInt(val.Value.(string), 10, 64)
		}

		p.Status = SourceStatusRunning

	default:
		return errors.New("task not implemented")
	}
	return nil
}

// NewConn create new pg connection
func (p *PostgreSQLCDC) NewConn() (*pgconn.PgConn, error) {
	dsn := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?replication=database&sslmode=%s", p.config.User, p.config.Password, p.config.Host, p.config.Port, p.config.Database, p.config.SslMode)

	conn, err := pgconn.Connect(p.ctx, dsn)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

// StartReplication Start Replication
func (p *PostgreSQLCDC) StartReplication() {
	var err error
	p.conn, err = p.NewConn()
	if err != nil {
		log.WithError(err).Error("Unable to start replication")
		return
	}

	sysident, err := pglogrepl.IdentifySystem(p.ctx, p.conn)
	if err != nil {
		log.Fatalln("IdentifySystem failed:", err)
	}
	log.WithFields(
		log.Fields{
			"SystemID": sysident.SystemID,
			"Timeline": sysident.Timeline,
			"XLogPos":  sysident.XLogPos,
			"DBName":   sysident.DBName,
		}).Info("IdentifySystem")

	p.meta.CommittedLsn, _ = p.GetConfirmedFlushLsn()
	// start replication
	log.WithField("offset", p.meta.CommittedLsn).Debug("Starting replication")

	options := pglogrepl.StartReplicationOptions{
		PluginArgs: []string{},
		Mode:       pglogrepl.LogicalReplication,
	}

	err = pglogrepl.StartReplication(p.ctx, p.conn, p.config.SlotName, pglogrepl.LSN(0), options)
	if err != nil {
		//slot not created waiting for event
		for strings.Contains(err.Error(), "SQLSTATE 42704") {
			log.WithError(err).Warn("NewConn()")
			log.Warn("Waiting 5 seconds to try again")
			time.Sleep(5 * time.Second)
			err = pglogrepl.StartReplication(p.ctx, p.conn, p.config.SlotName, pglogrepl.LSN(0), options)
		}
		log.WithError(err).Fatal("Unable to Start Replication")
	}
}

// checkStatus check Status
func (p *PostgreSQLCDC) checkStatus(force bool) {
	var err error
	// Stream closed
	if p.conn.IsClosed() {
		//reconnect
		log.Debug("NewTicker Reconnecting...")
		p.conn, err = p.NewConn()
		if err != nil {
			log.WithError(err).Fatal("NewConn()")
		}
	}

	p.meta.SlotStatus = p.GetSlotStatus()
	if !p.meta.SlotStatus {
		p.StartReplication()
	}

	standbyStatusUpdate := pglogrepl.StandbyStatusUpdate{}
	if force {
		standbyStatusUpdate.WALWritePosition = p.meta.ServerWALEnd
		p.meta.CommittedLsn = p.meta.ServerWALEnd
	} else {
		standbyStatusUpdate.WALWritePosition = p.meta.CommittedLsn
	}

	err = pglogrepl.SendStandbyStatusUpdate(p.ctx, p.conn, standbyStatusUpdate)
	if err != nil {
		log.WithError(err).Error("SendStandbyStatusUpdate failed")
	}
	log.WithFields(log.Fields{
		"committedLsn": p.meta.CommittedLsn,
		"currentLsn":   p.meta.CurrentLsn,
		"SlotStatus":   p.meta.SlotStatus,
	}).Info("Send agent Status")
}

// decodeEvents Send all statement for this transaction
func (p *PostgreSQLCDC) decodeEvents() {
	var err error
	var msgs *Messages
	var repMsg pgproto3.BackendMessage

	standbyTimeout := time.Second * TickerValue
	nextStatusSend := time.Now().Add(standbyTimeout)
	processEvent := false
	for {
		if time.Now().After(nextStatusSend) {
			p.checkStatus(p.CommittedState.IsEmpty())
			nextStatusSend = time.Now().Add(standbyTimeout)
		}
		if processEvent {
			continue
		}
		ctx, cancel := context.WithDeadline(p.ctx, nextStatusSend)
		repMsg, err = p.conn.ReceiveMessage(ctx)
		cancel()
		if err != nil {
			if pgconn.Timeout(err) {
				continue
			}
			if p.conn.IsClosed() {
				//reconnect
				log.Warn("Reconnecting...")
				p.conn, err = p.NewConn()
				if err != nil {
					log.WithError(err).Fatal("connection dead")
				}
				p.StartReplication()
			}
			log.WithError(err).Error("Error decoding event")
			continue
		} else if repMsg == nil {
			//do noting
			continue
		}

		switch msg := repMsg.(type) {
		case *pgproto3.CopyData:
			switch msg.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
				if err != nil {
					log.WithError(err).Fatal("Parse PrimaryKeepaliveMessage failed")
				}
				p.meta.ServerWALEnd = pkm.ServerWALEnd
				p.meta.CurrentLsn = p.meta.ServerWALEnd
				log.WithFields(
					log.Fields{
						"ServerWALEnd":   pkm.ServerWALEnd,
						"ServerTime":     pkm.ServerTime,
						"ReplyRequested": pkm.ReplyRequested,
					}).Debug("Primary Keepalive Message")

				if pkm.ReplyRequested {
					nextStatusSend = time.Time{}
				}
			case pglogrepl.StandbyStatusUpdateByteID:
				nextStatusSend = time.Time{}
			case pglogrepl.XLogDataByteID:
				xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
				if err != nil {
					log.WithError(err).Fatal("ParseXLogData failed:", err)
				}
				p.meta.CurrentLsn = xld.WALStart
				msgs = &Messages{}
				err = json.Unmarshal(utils.EscapeCtrl(xld.WALData), msgs)
				if err != nil {
					log.WithFields(log.Fields{
						"error": err,
						"json":  string(xld.WALData),
					}).Error("failed parse to JSON from PG")
					continue
				} else {
					processEvent = true
					go func() {
						defer func() {
							processEvent = false
						}()
						p.processMsgs(msgs, xld.ServerTime.UnixNano())
					}()
				}
				p.meta.CurrentLsn = xld.WALStart + pglogrepl.LSN(len(xld.WALData))
			}
		default:
			log.Printf("Received unexpected message: %#v\n", msg)
		}
	}
}

//processMsgs process Messages
func (p *PostgreSQLCDC) processMsgs(msgs *Messages, serverTime int64) {
	var timestamp int64

	for _, msg := range msgs.Change {
		p.meta.LastState = "Waiting for next pg event"

		if p.filter.IsFilteredTable(msg.Schema, msg.Table) {
			continue
		}

		//when servertime == 0 send current time
		if serverTime == 0 {
			timestamp = time.Now().UnixNano()
		} else {
			timestamp = serverTime
		}

		statement, OldStatement, columTypes, key := p.fieldsToMap(msg)
		p.CommittedState.Add(p.meta.CurrentLsn)
		p.OutputChannel <- events.LookatchEvent{
			Header: events.LookatchHeader{
				EventType: PostgreSQLCDCType,
				Tenant:    p.AgentInfo.Tenant,
			},
			Payload: events.SQLEvent{

				Timestamp:    strconv.FormatInt(timestamp, 10),
				Environment:  p.AgentInfo.Tenant.Env,
				Database:     p.config.Database,
				Schema:       msg.Schema,
				Table:        msg.Table,
				Method:       strings.ToLower(msg.Kind),
				Statement:    statement,
				OldStatement: OldStatement,
				ColumnsMeta:  columTypes,
				PrimaryKey:   key,
				Offset: &events.Offset{
					Source: p.meta.CurrentLsn.String(),
					Agent:  strconv.FormatInt(p.Offset, 10),
				},
			},
		}
		log.WithField("table", msg.Table).Debug("Event send")
	}
}

// fieldsToJSON map fields to json
func (p *PostgreSQLCDC) fieldsToMap(msg Message) (map[string]interface{}, map[string]interface{}, map[string]events.ColumnsMeta, string) {
	var key string
	var columnNames []string
	var columnTypes []string
	var columnValues []interface{}

	if msg.Kind == "delete" {
		columnNames = msg.Oldkeys.Keynames
		columnValues = msg.Oldkeys.Keyvalues
		columnTypes = msg.Oldkeys.Keytypes
	} else {
		columnNames = msg.Columnnames
		columnValues = msg.Columnvalues
		columnTypes = msg.Columntypes
	}

	s := make(map[string]interface{})
	o := make(map[string]interface{})
	c := make(map[string]events.ColumnsMeta)

	if p.config.OldValue {
		if msg.Kind != "insert" {
			for index, element := range msg.Oldkeys.Keynames {
				if !p.filter.IsFilteredColumn(msg.Schema, msg.Table, element) {
					o[element] = msg.Oldkeys.Keyvalues[index]
				}
			}
		}
	}
	keys := make([]string, 0)
	for index, element := range columnNames {
		if !p.filter.IsFilteredColumn(msg.Schema, msg.Table, element) {
			s[element] = columnValues[index]
			if p.config.ColumnsMetaValue {
				c[element] = events.ColumnsMeta{
					Type:     columnTypes[index],
					Position: index + 1,
				}
			}
			if ok := p.query.isPrimary(msg.Schema, msg.Table, strconv.Itoa(index)); ok {
				keys = append(keys, element)
			}

		}
	}
	if len(keys) == 0 {
		key = p.config.DefinedPk[msg.Schema+"."+msg.Table]
	} else {
		key = strings.Join(keys, ",")
	}

	return s, o, c, key
}

// GetSlotStatus get slot status
func (p *PostgreSQLCDC) GetSlotStatus() bool {
	// Fetch the restart LSN of the slot, to establish a starting point
	query := fmt.Sprintf("select active from pg_replication_slots where slot_name='%s'", p.config.SlotName)
	result, err := p.query.QueryMeta(query)
	if err != nil {
		log.Error("Error while getting Slot Status")
		return false
	}

	return result[0]["active"].(bool)
}

// GetRestartLsn Get restart Lsn
func (p *PostgreSQLCDC) GetConfirmedFlushLsn() (pglogrepl.LSN, error) {

	query := fmt.Sprintf("select confirmed_flush_lsn FROM pg_replication_slots where slot_name='%s'", p.config.SlotName)
	result, err := p.query.QueryMeta(query)
	if err != nil {
		return 0, err
	}
	if result == nil {
		return 0, fmt.Errorf("no commitedLsn found for slot '%s'", p.config.SlotName)
	}

	return pglogrepl.ParseLSN(result[0]["confirmed_flush_lsn"].(string))

}

// UpdateCommittedLsn  update CommittedLsn
func (p *PostgreSQLCDC) UpdateCommittedLsn() {
	for committedLsn := range p.CommitChannel {
		lsn, err := pglogrepl.ParseLSN(committedLsn.(string))
		if err != nil {
			log.WithError(err).Error("Error while updating committed Lsn")
		}
		p.CommittedState.CleanFromLsn(lsn)
		p.meta.CommittedLsn = lsn
	}
}

func NewOffsetCommittedState() *OffsetCommittedState {
	return &OffsetCommittedState{
		RWMutex:   sync.RWMutex{},
		SendedLsn: make([]pglogrepl.LSN, 0),
	}
}

//IsEmpty check if no offset in State
func (c *OffsetCommittedState) IsEmpty() bool {
	c.Lock()
	isEmpty := false
	if len(c.SendedLsn) == 0 {
		isEmpty = true
	}
	c.Unlock()
	return isEmpty
}

//Add add new lsn to state
func (c *OffsetCommittedState) Add(lsn pglogrepl.LSN) {
	c.Lock()
	c.SendedLsn = append(c.SendedLsn, lsn)
	c.Unlock()
}

//search return position of lsn in state, -1 otherwise
func (c *OffsetCommittedState) search(lsn pglogrepl.LSN) int {
	c.Lock()
	index := -1
	for ind, v := range c.SendedLsn {
		if v == lsn {
			index = ind
			break
		}
	}
	c.Unlock()
	return index
}

//CleanFromLsn clean old lsn state
func (c *OffsetCommittedState) CleanFromLsn(lsn pglogrepl.LSN) {
	pos := c.search(lsn)
	for pos != -1 {
		c.Lock()
		c.SendedLsn = c.SendedLsn[pos+1:]
		c.Unlock()
		pos = c.search(lsn)
	}

}
