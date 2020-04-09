package sources

import (
	"reflect"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/spf13/viper"
	"gopkg.in/guregu/null.v3"

	"github.com/Pirionfr/lookatch-agent/events"
)

var vPgcdc *viper.Viper
var sPgcdc *Source

func init() {
	vPgcdc = viper.New()
	vPgcdc.Set("agent.Hostname", "test")
	vPgcdc.Set("agent.env", "test")
	vPgcdc.Set("agent.UUID", "test")

	vPgcdc.Set("sources.default.autostart", true)
	vPgcdc.Set("sources.default.enabled", true)

	eventChan := make(chan events.LookatchEvent, 1)

	agentInfo := &AgentHeader{
		Tenant: events.LookatchTenantInfo{
			ID:  vPgcdc.GetString("agent.UUID"),
			Env: vPgcdc.GetString("agent.env"),
		},
		Hostname: vPgcdc.GetString("agent.Hostname"),
		UUID:     vPgcdc.GetString("agent.UUID"),
	}

	sPgcdc = &Source{
		Name:          "default",
		OutputChannel: eventChan,
		AgentInfo:     agentInfo,
		Conf:          vPgcdc,
	}

}

func TestPgcdcGetMeta(t *testing.T) {
	Pgcdc, ok := NewPostgreSQLCdc(sPgcdc)
	if ok != nil {
		t.Fail()
	}

	if len(Pgcdc.GetMeta()) == 0 {
		t.Fail()
	}
}

func TestPgcdcInit(t *testing.T) {
	Pgcdc, ok := NewPostgreSQLCdc(sPgcdc)
	if ok != nil {
		t.Fail()
	}

	Pgcdc.Init()
}

func TestPgcdcStop(t *testing.T) {
	Pgcdc, ok := NewPostgreSQLCdc(sPgcdc)
	if ok != nil {
		t.Fail()
	}

	if Pgcdc.Stop() != nil {
		t.Fail()
	}
}

func TestPgcdcGetName(t *testing.T) {
	Pgcdc, ok := NewPostgreSQLCdc(sPgcdc)
	if ok != nil {
		t.Fail()
	}

	if Pgcdc.GetName() != "default" {
		t.Fail()
	}
}

func TestPgcdcGetStatus(t *testing.T) {
	Pgcdc, ok := NewPostgreSQLCdc(sPgcdc)
	if ok != nil {
		t.Fail()
	}

	if Pgcdc.GetStatus() == nil {
		t.Fail()
	}
}

func TestPgcdcIsEnable(t *testing.T) {
	Pgcdc, ok := NewPostgreSQLCdc(sPgcdc)
	if ok != nil {
		t.Fail()
	}

	if !Pgcdc.IsEnable() {
		t.Fail()
	}
}

func TestPgcdcHealtCheck(t *testing.T) {
	Pgcdc, ok := NewPostgreSQLCdc(sPgcdc)
	if ok != nil {
		t.Fail()
	}

	if Pgcdc.HealthCheck() {
		t.Fail()
	}
}

func TestPgcdcGetAvailableActions(t *testing.T) {
	Pgcdc, ok := NewPostgreSQLCdc(sPgcdc)
	if ok != nil {
		t.Fail()
	}

	if Pgcdc.GetCapabilities() == nil {
		t.Fail()
	}
}

func TestPgcdcProcess(t *testing.T) {
	Pgcdc, ok := NewPostgreSQLCdc(sPgcdc)
	if ok != nil {
		t.Fail()
	}

	if Pgcdc.Process("") == nil {
		t.Fail()
	}
}

func TestPgcdcGetOutputChan(t *testing.T) {
	Pgcdc, ok := NewPostgreSQLCdc(sPgcdc)
	if ok != nil {
		t.Fail()
	}

	if reflect.TypeOf(Pgcdc.GetOutputChan()).String() != "chan events.LookatchEvent" {
		t.Fail()
	}
}

func TestGetSlotStatus(t *testing.T) {
	pgQuery, ok := NewPostgreSQLCdc(sPgcdc)
	if ok != nil {
		t.Fail()
	}
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	mock.ExpectQuery("select active from pg_replication_slots where slot_name='slot_test'").WillReturnRows(
		sqlmock.NewRows([]string{"active"}).
			AddRow(true))

	pCDC := pgQuery.(*PostgreSQLCDC)
	pCDC.query.db = db
	pCDC.config.SlotName = "slot_test"

	if !pCDC.GetSlotStatus() {
		t.Fail()
	}
}

func TestFieldsToMaps1(t *testing.T) {
	pgQuery, ok := NewPostgreSQLCdc(sPgcdc)
	if ok != nil {
		t.Fail()
	}
	pCDC := pgQuery.(*PostgreSQLCDC)
	pCDC.filter.FilterPolicy = "accept"
	pCDC.config.Database = "test"

	pCDC.query.schemas = SQLSchema{
		"SchemaTest": {
			"TableTest": {
				"0": &Column{
					Database:               "test",
					Schema:                 "SchemaTest",
					Table:                  "TableTest",
					Column:                 "col1",
					ColumnOrdPos:           0,
					Nullable:               false,
					DataType:               "INT4",
					CharacterMaximumLength: null.Int{},
					NumericPrecision:       null.Int{},
					NumericScale:           null.Int{},
					ColumnType:             "INT4",
					ColumnKey:              "PRI",
				},
			},
		},
	}
	msg := Message{
		Columnnames: []string{
			"col1",
		},
		Columntypes: []string{
			"INT4",
		},
		Columnvalues: []interface{}{
			1,
		},
		Kind:   "update",
		Schema: "SchemaTest",
		Table:  "TableTest",
		Oldkeys: Oldkeys{
			Keynames: []string{
				"col1",
			},
			Keytypes: []string{
				"INT4",
			},
			Keyvalues: []interface{}{
				2,
			},
		},
	}

	event, oldEvent, cMeta, pk := pCDC.fieldsToMap(msg)

	if event["col1"] != 1 {
		t.Fail()
	}

	if len(oldEvent) > 0 {
		t.Fail()
	}

	if len(cMeta) > 0 {
		t.Fail()
	}

	if pk != "col1" {
		t.Fail()
	}
}

func TestFieldsToMaps2(t *testing.T) {
	pgQuery, ok := NewPostgreSQLCdc(sPgcdc)
	if ok != nil {
		t.Fail()
	}
	pCDC := pgQuery.(*PostgreSQLCDC)
	pCDC.filter.FilterPolicy = "accept"
	pCDC.config.OldValue = true
	pCDC.config.ColumnsMetaValue = true
	msg := Message{
		Columnnames: []string{
			"col1",
		},
		Columntypes: []string{
			"INT4",
		},
		Columnvalues: []interface{}{
			1,
		},
		Kind:   "update",
		Schema: "SchemaTest",
		Table:  "TableTest",
		Oldkeys: Oldkeys{
			Keynames: []string{
				"col1",
			},
			Keytypes: []string{
				"INT4",
			},
			Keyvalues: []interface{}{
				2,
			},
		},
	}

	event, oldEvent, cMeta, _ := pCDC.fieldsToMap(msg)

	if event["col1"] != 1 {
		t.Fail()
	}

	if oldEvent["col1"] != 2 {
		t.Fail()
	}

	if cMeta["col1"].Type != "INT4" {
		t.Fail()
	}
}

func TestFieldsToMaps3(t *testing.T) {
	pgQuery, ok := NewPostgreSQLCdc(sPgcdc)
	if ok != nil {
		t.Fail()
	}
	pCDC := pgQuery.(*PostgreSQLCDC)
	pCDC.filter.FilterPolicy = "accept"
	msg := Message{
		Columnnames: []string{
			"col1",
		},
		Columntypes: []string{
			"INT4",
		},
		Columnvalues: []interface{}{
			1,
		},
		Kind:   "delete",
		Schema: "SchemaTest",
		Table:  "TableTest",
		Oldkeys: Oldkeys{
			Keynames: []string{
				"col1",
			},
			Keytypes: []string{
				"INT4",
			},
			Keyvalues: []interface{}{
				2,
			},
		},
	}

	event, oldEvent, cMeta, _ := pCDC.fieldsToMap(msg)

	if event["col1"] != 2 {
		t.Fail()
	}

	if len(oldEvent) > 0 {
		t.Fail()
	}

	if len(cMeta) > 0 {
		t.Fail()
	}
}

func TestProcessMsgs(t *testing.T) {
	pgQuery, ok := NewPostgreSQLCdc(sPgcdc)
	if ok != nil {
		t.Fail()
	}
	pCDC := pgQuery.(*PostgreSQLCDC)
	pCDC.filter.FilterPolicy = "accept"
	pCDC.config.OldValue = true
	pCDC.config.ColumnsMetaValue = true
	msg := Message{
		Columnnames: []string{
			"col1",
		},
		Columntypes: []string{
			"INT4",
		},
		Columnvalues: []interface{}{
			1,
		},
		Kind:   "update",
		Schema: "SchemaTest",
		Table:  "TableTest",
		Oldkeys: Oldkeys{
			Keynames: []string{
				"col1",
			},
			Keytypes: []string{
				"INT4",
			},
			Keyvalues: []interface{}{
				2,
			},
		},
	}
	msgs := &Messages{
		Change: []Message{
			msg,
		},
	}
	serverTime := time.Now().UnixNano()
	pCDC.processMsgs(msgs, serverTime)

	lk := <-pgQuery.GetOutputChan()

	if lk.Payload.(events.SQLEvent).Table != "TableTest" {
		t.Fail()
	}

	if lk.Payload.(events.SQLEvent).Statement["col1"] != 1 {
		t.Fail()
	}
}
