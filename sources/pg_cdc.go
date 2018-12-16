package sources

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Pirionfr/lookatch-agent/control"
	"github.com/Pirionfr/lookatch-agent/events"
	"github.com/Pirionfr/lookatch-agent/util"
	utils "github.com/Pirionfr/lookatch-agent/util"
	"github.com/jackc/pgx"
	log "github.com/sirupsen/logrus"
)

type (
	// PostgreSQLCDC representation of PostgreSQL change data capture
	PostgreSQLCDC struct {
		*Source
		config  PostgreSQLCDCConf
		query   *PostgreSQLQuery
		filter  *utils.Filter
		repConn *pgx.ReplicationConn
		status  string
		meta    Meta
	}

	// PostgreSQLCDCConf representation of PostgreSQL change data capture configuration
	PostgreSQLCDCConf struct {
		Host         string                 `json:"host"`
		Port         int                    `json:"port"`
		User         string                 `json:"user"`
		Password     string                 `json:"password"`
		SslMode      string                 `json:"sslmode"`
		Database     string                 `json:"database"`
		Offset       string                 `json:"offset"`
		SlotName     string                 `json:"slot_name" mapstructure:"slot_name"`
		OldValue     bool                   `json:"old_value" mapstructure:"old_value"`
		FilterPolicy string                 `json:"filter_policy" mapstructure:"filter_policy"`
		Filter       map[string]interface{} `json:"filter"`
		Enabled      bool                   `json:"enabled"`
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
		LastState  string `json:"laststate"`
		Lsn        uint64 `json:"offset"`
		SlotStatus bool   `json:"slotstatus"`
	}
)

// PostgreSQLCDCType type of source
const PostgreSQLCDCType = "postgresqlCDC"

// TickerValue number second to wait between to tick
const TickerValue = 10

// newPostgreSQLCdc create new PostgreSQL CDC source
func newPostgreSQLCdc(s *Source) (SourceI, error) {
	postgreSQLCDCConf := PostgreSQLCDCConf{}
	err := s.Conf.UnmarshalKey("sources."+s.Name, &postgreSQLCDCConf)
	if err != nil {
		return nil, err
	}

	query := &PostgreSQLQuery{
		JDBCQuery: &JDBCQuery{
			Source: s,
		},
		config: PostgreSQLQueryConfig{
			JDBCQueryConfig: &JDBCQueryConfig{
				Host:     postgreSQLCDCConf.Host,
				Port:     postgreSQLCDCConf.Port,
				User:     postgreSQLCDCConf.User,
				Password: postgreSQLCDCConf.Password,
				NbWorker: 1,
			},
			Database: postgreSQLCDCConf.Database,
			SslMode:  postgreSQLCDCConf.SslMode,
		},
	}
	p := &PostgreSQLCDC{
		Source: s,
		query:  query,
		config: postgreSQLCDCConf,
		status: control.SourceStatusWaitingForMETA,
		filter: &utils.Filter{
			FilterPolicy: postgreSQLCDCConf.FilterPolicy,
			Filter:       postgreSQLCDCConf.Filter,
		},
	}

	return p, nil
}

// Init source
func (p *PostgreSQLCDC) Init() {

	//start bi Query Schema
	err := p.query.QuerySchema()
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Error("Error while querying Schema")
		return
	}
}

// Stop source
func (p *PostgreSQLCDC) Stop() error {
	return nil
}

// Start source
func (p *PostgreSQLCDC) Start(i ...interface{}) (err error) {
	log.WithFields(log.Fields{
		"type": PostgreSQLCDCType,
	}).Debug("Start")

	if !util.IsStandalone(p.Conf) {
		var wg sync.WaitGroup
		wg.Add(1)
		//wait for changeStatus
		go func() {
			for p.status == control.SourceStatusWaitingForMETA {
				time.Sleep(time.Second)
			}
			wg.Done()
		}()
		wg.Wait()
	} else {
		p.status = control.SourceStatusRunning
	}

	p.repConn, err = p.NewReplicator()
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Fatal("NewReplicator error")
	}
	p.StartReplication()
	// consume events
	go p.checkStatus()
	go p.decodeEvents()

	return nil
}

// GetName get source name
func (p *PostgreSQLCDC) GetName() string {
	return p.Name
}

// GetOutputChan get output channel
func (p *PostgreSQLCDC) GetOutputChan() chan *events.LookatchEvent {
	return p.OutputChannel
}

// GetMeta get metadata
func (p *PostgreSQLCDC) GetMeta() map[string]interface{} {
	meta := make(map[string]interface{})
	if p.status != control.SourceStatusWaitingForMETA {
		meta["offset"] = p.meta.Lsn
		meta["offset_agent"] = p.Offset
		meta["slot_status"] = p.meta.SlotStatus
	}

	return meta
}

// IsEnable check if source is enable
func (p *PostgreSQLCDC) IsEnable() bool {
	return p.config.Enabled
}

// GetSchema get schema
func (p *PostgreSQLCDC) GetSchema() interface{} {
	return p.query.schemas
}

// GetStatus get status
func (p *PostgreSQLCDC) GetStatus() interface{} {
	return p.status
}

// HealthCheck returns true if ok
func (p *PostgreSQLCDC) HealthCheck() bool {
	return p.status == control.SourceStatusRunning && p.meta.SlotStatus
}

// GetAvailableActions returns available actions
func (p *PostgreSQLCDC) GetAvailableActions() map[string]*control.ActionDescription {
	availableAction := make(map[string]*control.ActionDescription)
	return availableAction
}

// Process action
func (p *PostgreSQLCDC) Process(action string, params ...interface{}) interface{} {

	switch action {
	case control.SourceMeta:
		meta := &control.Meta{}
		payload := params[0].([]byte)
		err := json.Unmarshal(payload, meta)
		if err != nil {
			log.WithFields(log.Fields{
				"error": err,
			}).Fatal("Unable to unmarshal MySQL Query Statement event :")
		} else {
			if val, ok := meta.Data["offset"]; ok {
				p.meta.Lsn = uint64(val.(float64))
			}

			if val, ok := meta.Data["offset_agent"]; ok {
				p.Offset = int64(val.(float64))
			}

			p.status = control.SourceStatusRunning
		}

	default:
		log.WithFields(log.Fields{
			"action": action,
		}).Error("action not implemented")
	}
	return nil
}

// NewReplicator create new pg logical decoding connection
func (p *PostgreSQLCDC) NewReplicator() (*pgx.ReplicationConn, error) {

	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s", p.config.Host, p.config.Port, p.config.User, p.config.Password, p.config.Database, p.config.SslMode)
	config, err := pgx.ParseDSN(dsn)
	if err != nil {
		return nil, err
	}

	conn, err := pgx.ReplicationConnect(config)
	if err != nil {
		return nil, err
	}
	if conn.IsAlive() {
		log.Debug("connection OK")
	}
	return conn, nil

}

// StartReplication Start Replication
func (p *PostgreSQLCDC) StartReplication() {
	// start replication
	log.WithFields(log.Fields{
		"offset": p.meta.Lsn,
	}).Debug("StartReplication")

	err := p.repConn.StartReplication(p.config.SlotName, p.meta.Lsn, -1)
	if err != nil {
		//slot not created waiting for event
		for strings.Contains(err.Error(), "SQLSTATE 42704") {
			log.Warn("NewReplicator()", err.Error())
			log.Warn("Waiting 5 seconds to try again")
			time.Sleep(5 * time.Second)
			err = p.repConn.StartReplication(p.config.SlotName, p.meta.Lsn, -1)
		}
		log.WithFields(log.Fields{
			"error": err,
		}).Fatal("Unable to StartReplication")
		os.Exit(1)
	}
}

// checkStatus check Status
func (p *PostgreSQLCDC) checkStatus() {
	var err error
	defer log.WithFields(log.Fields{
		"LastOffset": err,
	}).Info("Stop writing Lsn on file")
	for range (time.NewTicker(time.Second * TickerValue)).C {

		// Stream closed
		if !p.repConn.IsAlive() {
			//reconnect
			log.Debug("NewTicker Reconnecting...")
			p.repConn, err = p.NewReplicator()
			if err != nil {
				log.WithFields(log.Fields{
					"error": err,
				}).Fatal("NewReplicator()")
			}

		}

		p.meta.SlotStatus = p.getSlotStatus()
		if !p.meta.SlotStatus {
			p.StartReplication()
		}

		//send standbyStatus
		log.WithFields(log.Fields{
			"offset":     p.meta.Lsn,
			"SlotStatus": p.meta.SlotStatus,
		}).Info("Send agent Status")
		standbyStatus, _ := pgx.NewStandbyStatus(p.meta.Lsn)
		err := p.repConn.SendStandbyStatus(standbyStatus)
		if err != nil {
			log.WithFields(log.Fields{
				"error": err,
			}).Error("Error waiting for replication message")
		}
	}
}

// decodeEvents Send all statement for this transaction
func (p *PostgreSQLCDC) decodeEvents() {

	var err error
	var msgs *Messages
	var repMsg *pgx.ReplicationMessage

	for {
		repMsg, err = p.repConn.WaitForReplicationMessage(context.Background())
		if err != nil {
			//check connection
			if !p.repConn.IsAlive() {
				//reconnect
				log.Debug("Reconnecting...")
				p.repConn, err = p.NewReplicator()
				if err != nil {
					errMsg := "NewReplicator() : "
					log.Fatal(errMsg, err.Error())
				}
				p.StartReplication()
			}
			log.WithFields(log.Fields{
				"error": err,
			}).Error("Error decoding event")
		} else if repMsg == nil {
			//do noting
		} else if repMsg.ServerHeartbeat != nil {
			// If 1, the server is requesting a standby status message
			// to be sent immediately.
			if repMsg.ServerHeartbeat.ServerTime == 1 {
				standbyStatus, _ := pgx.NewStandbyStatus(p.meta.Lsn)
				err := p.repConn.SendStandbyStatus(standbyStatus)
				if err != nil {
					log.WithFields(log.Fields{
						"error": err,
					}).Error("Error waiting for replication message")
				}
			}
			p.meta.LastState = repMsg.ServerHeartbeat.String()

		} else if repMsg.WalMessage != nil && repMsg.WalMessage.WalData != nil {
			msgs = &Messages{}

			err = json.Unmarshal(util.EscapeCtrl(repMsg.WalMessage.WalData), msgs)
			if err != nil {
				//TODO maybe do fatal
				log.WithFields(log.Fields{
					"error": err,
					"json":  string(repMsg.WalMessage.WalData),
				}).Error("failed parse to JSON from PG")
				continue
			} else {
				p.processMsgs(msgs)
			}
			//set the current position
			if p.meta.Lsn < repMsg.WalMessage.WalStart {
				p.meta.Lsn = repMsg.WalMessage.WalStart
				p.Offset++
			}

		}
	}

}

//processMsgs process Messages
func (p *PostgreSQLCDC) processMsgs(msgs *Messages){
	var fields string
	var err error
	var ev *events.LookatchEvent
	var repMsg *pgx.ReplicationMessage
	var timestamp int64
	var key string

	for _, msg := range msgs.Change {

		p.meta.LastState = "Waiting for next pg event"

		if p.filter.IsFilteredTable(msg.Schema, msg.Table) {
			continue
		}

		fields, err = p.fieldsToJSON(msg, &key)
		if err != nil {
			log.WithFields(log.Fields{
				"error": err,
			}).Error("Failed to parse statement")
			continue
		}

		//when servertime == 0 send current time
		if repMsg.WalMessage.ServerTime == 0 {
			timestamp = time.Now().Unix()
		} else {
			timestamp = repMsg.WalMessage.Time().Unix()
		}

		ev = &events.LookatchEvent{
			Header: &events.LookatchHeader{
				EventType: MysqlCDCType,
				Tenant:    p.AgentInfo.tenant,
			},
			Payload: &events.SQLEvent{

				Timestamp:   strconv.FormatInt(timestamp, 10),
				Environment: p.AgentInfo.tenant.Env,
				Database:    p.config.Database,
				Schema:      msg.Schema,
				Table:       msg.Table,
				Method:      strings.ToLower(msg.Kind),
				Statement:   fields,
				PrimaryKey:  key,
				Offset: &events.Offset{
					Database: strconv.FormatUint(p.meta.Lsn, 10),
					Agent:    strconv.FormatInt(p.Offset, 10),
				},
			},
		}
		log.WithFields(log.Fields{
			"table": msg.Table,
		}).Debug("Event send")
		p.OutputChannel <- ev
	}
}

// fieldsToJSON map fields to json
func (p *PostgreSQLCDC) fieldsToJSON(msg Message, key *string) (string, error) {

	var columnnames []string
	var columnvalues []interface{}

	if msg.Kind == "delete" {
		columnnames = msg.Oldkeys.Keynames
		columnvalues = msg.Oldkeys.Keyvalues
	} else {
		columnnames = msg.Columnnames
		columnvalues = msg.Columnvalues
	}

	m := make(map[string]interface{})
	for index, element := range columnnames {
		if !p.filter.IsFilteredColumn(msg.Schema, msg.Table, element) {
			if ok := p.query.isPrimary(msg.Schema, msg.Table, strconv.Itoa(index)); ok && *key == "" {
				*key = element
			}
			m[element] = columnvalues[index]
		}
	}
	return fieldsMap2Json(m)
}

// fieldsMap2Json Convert a map key -> value into json
func fieldsMap2Json(i map[string]interface{}) (string, error) {

	j, err := json.Marshal(i)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Error("Failed to parse map statement")
	}
	return string(j), err
}
