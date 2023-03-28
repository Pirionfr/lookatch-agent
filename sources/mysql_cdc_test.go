package sources

import (
	"github.com/DATA-DOG/go-sqlmock"
	"reflect"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/spf13/viper"

	"github.com/Pirionfr/lookatch-agent/events"
)

var vMysqlcdc *viper.Viper
var sMysqlcdc *Source

func init() {
	vMysqlcdc = viper.New()
	vMysqlcdc.Set("agent.Hostname", "test")
	vMysqlcdc.Set("agent.env", "test")
	vMysqlcdc.Set("agent.UUID", "test")

	vMysqlcdc.Set("sources.default.autostart", true)
	vMysqlcdc.Set("sources.default.enabled", true)

	eventChan := make(chan events.LookatchEvent, 1)

	agentInfo := &AgentHeader{
		Tenant: events.LookatchTenantInfo{
			ID:  vMysqlcdc.GetString("agent.UUID"),
			Env: vMysqlcdc.GetString("agent.env"),
		},
		Hostname: vMysqlcdc.GetString("agent.Hostname"),
		UUID:     vMysqlcdc.GetString("agent.UUID"),
	}

	sMysqlcdc = &Source{
		Name:          "default",
		OutputChannel: eventChan,
		AgentInfo:     agentInfo,
		Conf:          vMysqlcdc,
	}

}

func TestMysqlcdcGetMeta(t *testing.T) {
	Mysqlcdc, ok := NewMysqlCdc(sMysqlcdc)
	if ok != nil {
		t.Fail()
	}

	if len(Mysqlcdc.GetMeta()) == 0 {
		t.Fail()
	}
}

func TestMysqlcdcGetMeta2(t *testing.T) {
	Mysqlcdc, ok := NewMysqlCdc(sMysqlcdc)
	if ok != nil {
		t.Fail()
	}

	if len(Mysqlcdc.GetMeta()) == 0 {
		t.Fail()
	}
}

func TestMysqlcdcInit(t *testing.T) {
	Mysqlcdc, ok := NewMysqlCdc(sMysqlcdc)
	if ok != nil {
		t.Fail()
	}

	Mysqlcdc.Init()
}

func TestMysqlcdcStop(t *testing.T) {
	Mysqlcdc, ok := NewMysqlCdc(sMysqlcdc)
	if ok != nil {
		t.Fail()
	}

	if Mysqlcdc.Stop() != nil {
		t.Fail()
	}
}

//TODO add standalone mode
//func TestMysqlcdcStart(t *testing.T) {
//	Mysqlcdc, ok := NewMysqlCdc(sMysqlcdc)
//	if ok != nil {
//		t.Fail()
//	}
//
//	if Mysqlcdc.Start() != nil {
//		t.Fail()
//	}
//}

func TestMysqlcdcGetName(t *testing.T) {
	Mysqlcdc, ok := NewMysqlCdc(sMysqlcdc)
	if ok != nil {
		t.Fail()
	}

	if Mysqlcdc.GetName() != "default" {
		t.Fail()
	}
}

func TestMysqlcdcGetStatus(t *testing.T) {
	Mysqlcdc, ok := NewMysqlCdc(sMysqlcdc)
	if ok != nil {
		t.Fail()
	}

	if Mysqlcdc.GetStatus() == nil {
		t.Fail()
	}
}

func TestMysqlcdcIsEnable(t *testing.T) {
	Mysqlcdc, ok := NewMysqlCdc(sMysqlcdc)
	if ok != nil {
		t.Fail()
	}

	if !Mysqlcdc.IsEnable() {
		t.Fail()
	}
}

func TestMysqlcdcHealtCheck(t *testing.T) {
	Mysqlcdc, ok := NewMysqlCdc(sMysqlcdc)
	if ok != nil {
		t.Fail()
	}

	if Mysqlcdc.HealthCheck() {
		t.Fail()
	}
}

func TestMysqlcdcGetAvailableActions(t *testing.T) {
	Mysqlcdc, ok := NewMysqlCdc(sMysqlcdc)
	if ok != nil {
		t.Fail()
	}

	if Mysqlcdc.GetCapabilities() == nil {
		t.Fail()
	}
}

func TestMysqlcdcProcess(t *testing.T) {
	Mysqlcdc, ok := NewMysqlCdc(sMysqlcdc)
	if ok != nil {
		t.Fail()
	}

	if Mysqlcdc.Process("") == nil {
		t.Fail()
	}
}

func TestMysqlcdcGetOutputChan(t *testing.T) {
	Mysqlcdc, ok := NewMysqlCdc(sMysqlcdc)
	if ok != nil {
		t.Fail()
	}

	if reflect.TypeOf(Mysqlcdc.GetOutputChan()).String() != "chan events.LookatchEvent" {
		t.Fail()
	}
}

func TestOnPosSynced(t *testing.T) {

	Mysqlcdc, ok := NewMysqlCdc(sMysqlcdc)
	if ok != nil {
		t.Fail()
	}
	mysqlCDC := Mysqlcdc.(*MysqlCDC)
	mysqlCDC.config.Mode = ModeBinlog
	mysqlCDC.cdcOffset.Update(mysql.Position{Pos: 3, Name: "test"})

	if mysqlCDC.OnPosSynced(nil, mysql.Position{Pos: 5, Name: "test"}, nil, true) != nil {
		t.Fail()
	}

	if mysqlCDC.cdcOffset.Position().Compare(mysql.Position{Pos: 5, Name: "test"}) != 0 {
		t.Fail()
	}
}

func TestOnPosSynced2(t *testing.T) {

	Mysqlcdc, ok := NewMysqlCdc(sMysqlcdc)
	if ok != nil {
		t.Fail()
	}
	mysqlCDC := Mysqlcdc.(*MysqlCDC)
	mysqlCDC.config.Mode = ModeBinlog

	gSet, _ := mysql.ParseMariadbGTIDSet("0-1-1")
	newGSet, _ := mysql.ParseMariadbGTIDSet("0-1-2")

	mysqlCDC.cdcOffset.UpdateGTIDSet(gSet)

	if mysqlCDC.OnPosSynced(nil, mysql.Position{}, newGSet, true) != nil {
		t.Fail()
	}

	if !mysqlCDC.cdcOffset.GTIDSet().Equal(newGSet) {
		t.Fail()
	}
}

func TestOnXID(t *testing.T) {

	Mysqlcdc, ok := NewMysqlCdc(sMysqlcdc)
	if ok != nil {
		t.Fail()
	}
	mysqlCDC := Mysqlcdc.(*MysqlCDC)
	mysqlCDC.cdcOffset.Update(mysql.Position{Pos: 3, Name: "test"})

	if mysqlCDC.OnXID(nil, mysql.Position{Pos: 5, Name: "test"}) != nil {
		t.Fail()
	}

	if mysqlCDC.cdcOffset.Position().Compare(mysql.Position{Pos: 5, Name: "test"}) != 0 {
		t.Fail()
	}
}

func TestOnGTID(t *testing.T) {

	Mysqlcdc, ok := NewMysqlCdc(sMysqlcdc)
	if ok != nil {
		t.Fail()
	}
	mysqlCDC := Mysqlcdc.(*MysqlCDC)

	gSet, _ := mysql.ParseMariadbGTIDSet("0-1-1")
	newGSet, _ := mysql.ParseMariadbGTIDSet("0-1-2")

	mysqlCDC.cdcOffset.UpdateGTIDSet(gSet)

	if mysqlCDC.OnGTID(nil, newGSet) != nil {
		t.Fail()
	}

	if !mysqlCDC.cdcOffset.GTIDSet().Equal(newGSet) {
		t.Fail()
	}
}

func TestOnRotate(t *testing.T) {

	Mysqlcdc, ok := NewMysqlCdc(sMysqlcdc)
	if ok != nil {
		t.Fail()
	}
	mysqlCDC := Mysqlcdc.(*MysqlCDC)
	mysqlCDC.cdcOffset.Update(mysql.Position{Pos: 3, Name: "test"})

	if mysqlCDC.OnRotate(nil, &replication.RotateEvent{Position: 4, NextLogName: []byte("test2")}) != nil {
		t.Fail()
	}

	if mysqlCDC.cdcOffset.Position().Compare(mysql.Position{Pos: 4, Name: "test2"}) != 0 {
		t.Fail()
	}
}

func TestOnTableChanged(t *testing.T) {

	Mysqlcdc, ok := NewMysqlCdc(sMysqlcdc)
	if ok != nil {
		t.Fail()
	}
	mysqlCDC := Mysqlcdc.(*MysqlCDC)

	if mysqlCDC.OnTableChanged(nil, "", "") != nil {
		t.Fail()
	}
}

func TestOnDDL(t *testing.T) {

	Mysqlcdc, ok := NewMysqlCdc(sMysqlcdc)
	if ok != nil {
		t.Fail()
	}
	mysqlCDC := Mysqlcdc.(*MysqlCDC)

	if mysqlCDC.OnDDL(nil, mysql.Position{}, nil) != nil {
		t.Fail()
	}
}

func TestLastBinlog(t *testing.T) {

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()
	rows := sqlmock.NewRows([]string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB"}).
		AddRow("mysqld-bin.000003", 2319, "", "")

	mock.ExpectQuery("SHOW MASTER STATUS").WillReturnRows(rows)

	Mysqlcdc, ok := NewMysqlCdc(sMysqlcdc)
	if ok != nil {
		t.Fail()
	}
	mysqlCDC := Mysqlcdc.(*MysqlCDC)
	mysqlCDC.query.db = db

	pos, err := mysqlCDC.GetLastBinlog()
	if err != nil {
		t.Fail()
	}

	if pos.Pos != 2319 || pos.Name != "mysqld-bin.000003" {
		t.Fail()
	}

}

func TestFirstBinlog(t *testing.T) {

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()
	rows := sqlmock.NewRows([]string{"Log_name", "Pos"}).
		AddRow("mysqld-bin.000003", 4)

	mock.ExpectQuery("SHOW BINLOG EVENTS limit 1").WillReturnRows(rows)

	Mysqlcdc, ok := NewMysqlCdc(sMysqlcdc)
	if ok != nil {
		t.Fail()
	}
	mysqlCDC := Mysqlcdc.(*MysqlCDC)
	mysqlCDC.query.db = db

	pos, err := mysqlCDC.GetFirstBinlog()
	if err != nil {
		t.Fail()
	}

	if pos.Pos != 4 || pos.Name != "mysqld-bin.000003" {
		t.Fail()
	}

}

func TestGetValidMysqlGTIDFromOffset(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()
	rows := sqlmock.NewRows([]string{"@@gtid_purged"}).
		AddRow("5fbbe9b1-5c79-11ea-81ce-0242ac110002:1-15")

	mock.ExpectQuery("SELECT @@gtid_purged").WillReturnRows(rows)

	Mysqlcdc, ok := NewMysqlCdc(sMysqlcdc)
	if ok != nil {
		t.Fail()
	}
	mysqlCDC := Mysqlcdc.(*MysqlCDC)
	mysqlCDC.query.db = db

	set, err := mysqlCDC.GetValidMysqlGTIDFromOffset("5fbbe9b1-5c79-11ea-81ce-0242ac110002:50")
	if err != nil {
		t.Fail()
	}

	if set.String() != "5fbbe9b1-5c79-11ea-81ce-0242ac110002:1-50" {
		t.Fail()
	}
}

func TestGetValidMysqlGTIDFromOffsetMalformed(t *testing.T) {
	Mysqlcdc, ok := NewMysqlCdc(sMysqlcdc)
	if ok != nil {
		t.Fail()
	}
	mysqlCDC := Mysqlcdc.(*MysqlCDC)

	_, err := mysqlCDC.GetValidMysqlGTIDFromOffset("1-1-1")
	if err == nil {
		t.Fail()
	}
}

func TestGetGTIDFromMariaDBPosition(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	mock.ExpectQuery(`SELECT BINLOG_GTID_POS\("mysqld-bin.000003", 4\) as GTID`).WillReturnRows(
		sqlmock.NewRows([]string{"GTID"}).
			AddRow("0-1-234"))

	Mysqlcdc, ok := NewMysqlCdc(sMysqlcdc)
	if ok != nil {
		t.Fail()
	}
	mysqlCDC := Mysqlcdc.(*MysqlCDC)
	mysqlCDC.query.db = db

	set, err := mysqlCDC.GetGTIDFromMariaDBPosition(mysql.Position{
		Name: "mysqld-bin.000003",
		Pos:  4,
	})
	if err != nil {
		t.Fail()
	}

	if set == nil || set.String() != "0-1-234" {
		t.Fail()
	}
}

func TestGetValidMariaDBGTIDFromOffset1(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	mock.ExpectQuery("SHOW BINLOG EVENTS limit 1").WillReturnRows(
		sqlmock.NewRows([]string{"Log_name", "Pos"}).
			AddRow("mysqld-bin.000003", 4))

	mock.ExpectQuery(`SELECT BINLOG_GTID_POS\("mysqld-bin.000003", 4\) as GTID`).WillReturnRows(
		sqlmock.NewRows([]string{"GTID"}).
			AddRow("0-1-234"))

	mock.ExpectQuery("SHOW MASTER STATUS").WillReturnRows(
		sqlmock.NewRows([]string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB"}).
			AddRow("mysqld-bin.000003", 2319, "", ""))

	mock.ExpectQuery(`SELECT BINLOG_GTID_POS\("mysqld-bin.000003", 2319\) as GTID`).WillReturnRows(
		sqlmock.NewRows([]string{"GTID"}).
			AddRow("0-1-2200"))

	mock.ExpectQuery("SELECT @@gtid_binlog_pos").WillReturnRows(
		sqlmock.NewRows([]string{"@@gtid_binlog_pos"}).
			AddRow("0-1-2200"))

	Mysqlcdc, ok := NewMysqlCdc(sMysqlcdc)
	if ok != nil {
		t.Fail()
	}
	mysqlCDC := Mysqlcdc.(*MysqlCDC)
	mysqlCDC.query.db = db

	set, err := mysqlCDC.GetValidMariaDBGTIDFromOffset("0-1-1")
	if err != nil {
		t.Fail()
	}

	if set == nil || set.String() != "0-1-234" {
		t.Fail()
	}
}

func TestGetValidMariaDBGTIDFromOffset2(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	mock.ExpectQuery("SHOW BINLOG EVENTS limit 1").WillReturnRows(
		sqlmock.NewRows([]string{"Log_name", "Pos"}).
			AddRow("mysqld-bin.000003", 4))

	mock.ExpectQuery(`SELECT BINLOG_GTID_POS\("mysqld-bin.000003", 4\) as GTID`).WillReturnRows(
		sqlmock.NewRows([]string{"GTID"}).
			AddRow("0-1-234"))

	mock.ExpectQuery("SHOW MASTER STATUS").WillReturnRows(
		sqlmock.NewRows([]string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB"}).
			AddRow("mysqld-bin.000003", 2319, "", ""))

	mock.ExpectQuery(`SELECT BINLOG_GTID_POS\("mysqld-bin.000003", 2319\) as GTID`).WillReturnRows(
		sqlmock.NewRows([]string{"GTID"}).
			AddRow("0-1-2200"))

	mock.ExpectQuery("SELECT @@gtid_binlog_pos").WillReturnRows(
		sqlmock.NewRows([]string{"@@gtid_binlog_pos"}).
			AddRow("0-1-2200,11-3-1"))

	Mysqlcdc, ok := NewMysqlCdc(sMysqlcdc)
	if ok != nil {
		t.Fail()
	}
	mysqlCDC := Mysqlcdc.(*MysqlCDC)
	mysqlCDC.query.db = db

	set, err := mysqlCDC.GetValidMariaDBGTIDFromOffset("0-1-235")
	if err != nil {
		t.Fail()
	}

	if set == nil || set.String() != "0-1-235,11-3-1" {
		t.Fail()
	}
}

func TestGetValidMariaDBGTIDFromOffset3(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	mock.ExpectQuery("SHOW BINLOG EVENTS limit 1").WillReturnRows(
		sqlmock.NewRows([]string{"Log_name", "Pos"}).
			AddRow("mysqld-bin.000003", 4))

	mock.ExpectQuery(`SELECT BINLOG_GTID_POS\("mysqld-bin.000003", 4\) as GTID`).WillReturnRows(
		sqlmock.NewRows([]string{"GTID"}).
			AddRow("0-1-234"))

	mock.ExpectQuery("SHOW MASTER STATUS").WillReturnRows(
		sqlmock.NewRows([]string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB"}).
			AddRow("mysqld-bin.000003", 2319, "", ""))

	mock.ExpectQuery(`SELECT BINLOG_GTID_POS\("mysqld-bin.000003", 2319\) as GTID`).WillReturnRows(
		sqlmock.NewRows([]string{"GTID"}).
			AddRow("0-1-2200"))

	mock.ExpectQuery("SELECT @@gtid_binlog_pos").WillReturnRows(
		sqlmock.NewRows([]string{"@@gtid_binlog_pos"}).
			AddRow("0-1-2200"))

	Mysqlcdc, ok := NewMysqlCdc(sMysqlcdc)
	if ok != nil {
		t.Fail()
	}
	mysqlCDC := Mysqlcdc.(*MysqlCDC)
	mysqlCDC.query.db = db

	set, err := mysqlCDC.GetValidMariaDBGTIDFromOffset("0-1-5000")
	if err != nil {
		t.Fail()
	}

	if set == nil || set.String() != "0-1-2200" {
		t.Fail()
	}
}

func TestGetValidMariaDBGTIDFromOffset4(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	mock.ExpectQuery("SHOW BINLOG EVENTS limit 1").WillReturnRows(
		sqlmock.NewRows([]string{"Log_name", "Pos"}).
			AddRow("mysqld-bin.000003", 4))

	mock.ExpectQuery(`SELECT BINLOG_GTID_POS\("mysqld-bin.000003", 4\) as GTID`).WillReturnRows(
		sqlmock.NewRows([]string{"GTID"}).
			AddRow("0-1-234"))

	mock.ExpectQuery("SHOW MASTER STATUS").WillReturnRows(
		sqlmock.NewRows([]string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB"}).
			AddRow("mysqld-bin.000003", 2319, "", ""))

	mock.ExpectQuery(`SELECT BINLOG_GTID_POS\("mysqld-bin.000003", 2319\) as GTID`).WillReturnRows(
		sqlmock.NewRows([]string{"GTID"}).
			AddRow("0-1-2200"))

	mock.ExpectQuery("SELECT @@gtid_binlog_pos").WillReturnRows(
		sqlmock.NewRows([]string{"@@gtid_binlog_pos"}).
			AddRow("0-1-2200"))

	Mysqlcdc, ok := NewMysqlCdc(sMysqlcdc)
	if ok != nil {
		t.Fail()
	}
	mysqlCDC := Mysqlcdc.(*MysqlCDC)
	mysqlCDC.query.db = db

	set, err := mysqlCDC.GetValidMariaDBGTIDFromOffset("1-1-500")
	if err != nil {
		t.Fail()
	}

	if set == nil || set.String() != "0-1-234" {
		t.Fail()
	}
}

func TestGetValidBinlogFromOffset1(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	mock.ExpectQuery("SHOW BINLOG EVENTS limit 1").WillReturnRows(
		sqlmock.NewRows([]string{"Log_name", "Pos"}).
			AddRow("mysqld-bin.000003", 4))

	mock.ExpectQuery("SHOW MASTER STATUS").WillReturnRows(
		sqlmock.NewRows([]string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB"}).
			AddRow("mysqld-bin.000003", 2319, "", ""))

	Mysqlcdc, ok := NewMysqlCdc(sMysqlcdc)
	if ok != nil {
		t.Fail()
	}
	mysqlCDC := Mysqlcdc.(*MysqlCDC)
	mysqlCDC.query.db = db

	pos := mysqlCDC.GetValidBinlogFromOffset("mysqld-bin.000003:14:")

	if pos.Name != "mysqld-bin.000003" && pos.Pos != 14 {
		t.Fail()
	}
}

func TestGetValidBinlogFromOffset2(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	mock.ExpectQuery("SHOW BINLOG EVENTS limit 1").WillReturnRows(
		sqlmock.NewRows([]string{"Log_name", "Pos"}).
			AddRow("mysqld-bin.000003", 4))

	mock.ExpectQuery("SHOW MASTER STATUS").WillReturnRows(
		sqlmock.NewRows([]string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB"}).
			AddRow("mysqld-bin.000003", 2319, "", ""))

	Mysqlcdc, ok := NewMysqlCdc(sMysqlcdc)
	if ok != nil {
		t.Fail()
	}
	mysqlCDC := Mysqlcdc.(*MysqlCDC)
	mysqlCDC.query.db = db

	pos := mysqlCDC.GetValidBinlogFromOffset("mysqld-bin.000004:14:")

	if pos.Name != "mysqld-bin.000003" && pos.Pos != 4 {
		t.Fail()
	}
}

func TestGetValidBinlogFromOffset3(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	mock.ExpectQuery("SHOW BINLOG EVENTS limit 1").WillReturnRows(
		sqlmock.NewRows([]string{"Log_name", "Pos"}).
			AddRow("mysqld-bin.000003", 4))

	mock.ExpectQuery("SHOW MASTER STATUS").WillReturnRows(
		sqlmock.NewRows([]string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB"}).
			AddRow("mysqld-bin.000003", 2319, "", ""))

	Mysqlcdc, ok := NewMysqlCdc(sMysqlcdc)
	if ok != nil {
		t.Fail()
	}
	mysqlCDC := Mysqlcdc.(*MysqlCDC)
	mysqlCDC.query.db = db

	pos := mysqlCDC.GetValidBinlogFromOffset("mysqld-bin.000003:5000:")

	if pos.Name != "mysqld-bin.000003" && pos.Pos != 2319 {
		t.Fail()
	}
}

func TestGetValidBinlogFromOffset4(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	mock.ExpectQuery("SHOW BINLOG EVENTS limit 1").WillReturnRows(
		sqlmock.NewRows([]string{"Log_name", "Pos"}).
			AddRow("mysqld-bin.000003", 4))

	mock.ExpectQuery("SHOW MASTER STATUS").WillReturnRows(
		sqlmock.NewRows([]string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB"}).
			AddRow("mysqld-bin.000003", 2319, "", ""))

	Mysqlcdc, ok := NewMysqlCdc(sMysqlcdc)
	if ok != nil {
		t.Fail()
	}
	mysqlCDC := Mysqlcdc.(*MysqlCDC)
	mysqlCDC.query.db = db

	pos := mysqlCDC.GetValidBinlogFromOffset("1-1-1")

	if pos.Name != "mysqld-bin.000003" && pos.Pos != 4 {
		t.Fail()
	}
}
