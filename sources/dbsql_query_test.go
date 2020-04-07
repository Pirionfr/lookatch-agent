package sources

import "testing"

func TestExtractDatabaseTable(t *testing.T) {
	gdbc := DBSQLQuery{}

	database, table := gdbc.ExtractDatabaseTable("select * from \"sepaAccountInfo\" limit 10;")

	if table != "sepaAccountInfo" {
		t.Fail()
	}

	if database != "" {
		t.Fail()
	}

}

func TestExtractDatabaseTable1(t *testing.T) {
	m := &DBSQLQuery{}
	database, table := m.ExtractDatabaseTable("select * From database.table")
	if database != "database" || table != "table" {
		t.Fail()
	}
}

func TestExtractDatabaseTable2(t *testing.T) {
	m := &DBSQLQuery{}
	database, table := m.ExtractDatabaseTable("select * from database.table Where c1=10")
	if database != "database" || table != "table" {
		t.Fail()
	}
}

func TestExtractDatabaseTable3(t *testing.T) {
	m := &MySQLQuery{}
	database, table := m.ExtractDatabaseTable("select 1 from table")
	if database != "" || table != "table" {
		t.Fail()
	}
}

func TestExtractDatabaseTable4(t *testing.T) {
	m := &DBSQLQuery{}
	database, table := m.ExtractDatabaseTable("select * from 异体字")
	if database != "" || table != "异体字" {
		t.Fail()
	}
}

func TestExtractDatabaseTable5(t *testing.T) {
	m := &DBSQLQuery{}
	database, table := m.ExtractDatabaseTable("select 1")
	if database != "" || table != "" {
		t.Fail()
	}
}

func TestExtractDatabaseTable6(t *testing.T) {
	m := &DBSQLQuery{}
	database, table := m.ExtractDatabaseTable("select * from database.tableWhere Where c1=10")
	if database != "database" || table != "tableWhere" {
		t.Fail()
	}
}

func TestExtractDatabaseTable7(t *testing.T) {
	m := &DBSQLQuery{}
	database, table := m.ExtractDatabaseTable("select * from \"table\" Where c1=10")
	if database != "" || table != "table" {
		t.Fail()
	}
}

func TestExtractDatabaseTable8(t *testing.T) {
	m := &DBSQLQuery{}
	database, table := m.ExtractDatabaseTable("select * from \"database\".\"table\" Where c1=10")
	if database != "database" || table != "table" {
		t.Fail()
	}
}
