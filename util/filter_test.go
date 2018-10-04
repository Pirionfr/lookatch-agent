package util

import (
	"bytes"
	"testing"

	"github.com/spf13/viper"
)

func TestIsAccept(t *testing.T) {
	filter := &Filter{
		FilterPolicy: "drop",
		Filter:       nil,
	}
	if filter.isAccept() {
		t.Fail()
	}
}
func TestIsAccept2(t *testing.T) {
	filter := &Filter{
		FilterPolicy: "accept",
		Filter:       nil,
	}
	if !filter.isAccept() {
		t.Fail()
	}
}

func TestDrop(t *testing.T) {
	filter := &Filter{
		FilterPolicy: "drop",
		Filter:       nil,
	}
	if !filter.IsFilteredDatabase("test") {
		t.Fail()
	}
}

func TestAccept(t *testing.T) {
	filter := &Filter{
		FilterPolicy: "accept",
		Filter:       nil,
	}
	if filter.IsFilteredDatabase("test") {
		t.Fail()
	}
}

func TestDatabaseDropNull(t *testing.T) {
	filters := make(map[string]interface{})
	aViper := viper.New()
	aViper.SetConfigType("JSON")
	aViper.ReadConfig(bytes.NewBuffer([]byte(`{"filter" :{"test":null}}`)))
	aViper.UnmarshalKey("filter", &filters)
	filter := &Filter{
		FilterPolicy: "drop",
		Filter:       filters,
	}
	if filter.IsFilteredDatabase("test") {
		t.Fail()
	}
}

func TestDatabaseDropNull2(t *testing.T) {
	filters := make(map[string]interface{})
	aViper := viper.New()
	aViper.SetConfigType("JSON")
	aViper.ReadConfig(bytes.NewBuffer([]byte(`{"filter" :{"test":null}}`)))
	aViper.UnmarshalKey("filter", &filters)
	filter := &Filter{
		FilterPolicy: "drop",
		Filter:       filters,
	}
	if !filter.IsFilteredDatabase("test2") {
		t.Fail()
	}
}

func TestDatabaseAcceptNull(t *testing.T) {
	filters := make(map[string]interface{})
	aViper := viper.New()
	aViper.SetConfigType("JSON")
	aViper.ReadConfig(bytes.NewBuffer([]byte(`{"filter" :{"test":null}}`)))
	aViper.UnmarshalKey("filter", &filters)
	filter := &Filter{
		FilterPolicy: "accept",
		Filter:       filters,
	}
	if !filter.IsFilteredDatabase("test") {
		t.Fail()
	}
}

func TestDatabaseAccept2(t *testing.T) {
	filters := make(map[string]interface{})
	aViper := viper.New()
	aViper.SetConfigType("JSON")
	aViper.ReadConfig(bytes.NewBuffer([]byte(`{"filter" :{"test":null}}`)))
	aViper.UnmarshalKey("filter", &filters)
	filter := &Filter{
		FilterPolicy: "accept",
		Filter:       filters,
	}
	if filter.IsFilteredDatabase("test2") {
		t.Fail()
	}
}

func TestDatabaseDropTable(t *testing.T) {
	filters := make(map[string]interface{})
	aViper := viper.New()
	aViper.SetConfigType("JSON")
	aViper.ReadConfig(bytes.NewBuffer([]byte(`{"filter" :{"test":{"EMPLOYEE":null}}}`)))
	aViper.UnmarshalKey("filter", &filters)
	filter := &Filter{
		FilterPolicy: "drop",
		Filter:       filters,
	}
	if filter.IsFilteredDatabase("test") {
		t.Fail()
	}
}

func TestDatabaseAcceptTable(t *testing.T) {
	filters := make(map[string]interface{})
	aViper := viper.New()
	aViper.SetConfigType("JSON")
	aViper.ReadConfig(bytes.NewBuffer([]byte(`{"filter" :{"test":{"EMPLOYEE":null}}}`)))
	aViper.UnmarshalKey("filter", &filters)
	filter := &Filter{
		FilterPolicy: "accept",
		Filter:       filters,
	}
	if filter.IsFilteredDatabase("test") {
		t.Fail()
	}
}

func TestTableDrop(t *testing.T) {
	filters := make(map[string]interface{})
	aViper := viper.New()
	aViper.SetConfigType("JSON")
	aViper.ReadConfig(bytes.NewBuffer([]byte(`{"filter" :{"test":{"EMPLOYEE":null}}}`)))
	aViper.UnmarshalKey("filter", &filters)

	filter := &Filter{
		FilterPolicy: "drop",
		Filter:       filters,
	}
	if filter.IsFilteredTable("test", "EMPLOYEE") {
		t.Fail()
	}
}

func TestTableDrop2(t *testing.T) {
	filters := make(map[string]interface{})
	aViper := viper.New()
	aViper.SetConfigType("JSON")
	aViper.ReadConfig(bytes.NewBuffer([]byte(`{"filter" :{"test":{"EMPLOYEE":null}}}`)))
	aViper.UnmarshalKey("filter", &filters)

	filter := &Filter{
		FilterPolicy: "drop",
		Filter:       filters,
	}
	if !filter.IsFilteredTable("test2", "EMPLOYEE") {
		t.Fail()
	}
}

func TestTableDrop3(t *testing.T) {
	filters := make(map[string]interface{})
	aViper := viper.New()
	aViper.SetConfigType("JSON")
	aViper.ReadConfig(bytes.NewBuffer([]byte(`{"filter" :{"test":{"EMPLOYEE":null}}}`)))
	aViper.UnmarshalKey("filter", &filters)

	filter := &Filter{
		FilterPolicy: "drop",
		Filter:       filters,
	}
	if !filter.IsFilteredTable("test", "EMPLOYEE2") {
		t.Fail()
	}
}

func TestTableDrop4(t *testing.T) {
	filters := make(map[string]interface{})
	aViper := viper.New()
	aViper.SetConfigType("JSON")
	aViper.ReadConfig(bytes.NewBuffer([]byte(`{"filter" :{"test":{"EMPLOYEE":null}}}`)))
	aViper.UnmarshalKey("filter", &filters)

	filter := &Filter{
		FilterPolicy: "drop",
		Filter:       filters,
	}
	if !filter.IsFilteredTable("test2", "EMPLOYEE2") {
		t.Fail()
	}
}

func TestTableAcceptNull(t *testing.T) {
	filters := make(map[string]interface{})
	aViper := viper.New()
	aViper.SetConfigType("JSON")
	aViper.ReadConfig(bytes.NewBuffer([]byte(`{"filter" :{"test":{"EMPLOYEE":null}}}`)))
	aViper.UnmarshalKey("filter", &filters)
	filter := &Filter{
		FilterPolicy: "accept",
		Filter:       filters,
	}
	if !filter.IsFilteredTable("test", "EMPLOYEE") {
		t.Fail()
	}
}

func TestTableAcceptNull2(t *testing.T) {
	filters := make(map[string]interface{})
	aViper := viper.New()
	aViper.SetConfigType("JSON")
	aViper.ReadConfig(bytes.NewBuffer([]byte(`{"filter" :{"test":{"EMPLOYEE":null}}}`)))
	aViper.UnmarshalKey("filter", &filters)
	filter := &Filter{
		FilterPolicy: "accept",
		Filter:       filters,
	}
	if filter.IsFilteredTable("test2", "EMPLOYEE") {
		t.Fail()
	}
}

func TestTableAcceptNull3(t *testing.T) {
	filters := make(map[string]interface{})
	aViper := viper.New()
	aViper.SetConfigType("JSON")
	aViper.ReadConfig(bytes.NewBuffer([]byte(`{"filter" :{"test":{"EMPLOYEE":null}}}`)))
	aViper.UnmarshalKey("filter", &filters)
	filter := &Filter{
		FilterPolicy: "accept",
		Filter:       filters,
	}
	if filter.IsFilteredTable("test", "EMPLOYEE2") {
		t.Fail()
	}
}

func TestTableAcceptNull4(t *testing.T) {
	filters := make(map[string]interface{})
	aViper := viper.New()
	aViper.SetConfigType("JSON")
	aViper.ReadConfig(bytes.NewBuffer([]byte(`{"filter" :{"test":{"EMPLOYEE":null}}}`)))
	aViper.UnmarshalKey("filter", &filters)
	filter := &Filter{
		FilterPolicy: "accept",
		Filter:       filters,
	}
	if filter.IsFilteredTable("test2", "EMPLOYEE2") {
		t.Fail()
	}
}

func TestTableAcceptColumn(t *testing.T) {
	filters := make(map[string]interface{})
	aViper := viper.New()
	aViper.SetConfigType("JSON")
	aViper.ReadConfig(bytes.NewBuffer([]byte(`{"filter" :{"test":{"EMPLOYEE":["EMP_ID"]}}}`)))
	aViper.UnmarshalKey("filter", &filters)

	filter := &Filter{
		FilterPolicy: "accept",
		Filter:       filters,
	}
	if filter.IsFilteredTable("test", "EMPLOYEE") {
		t.Fail()
	}
}

func TestTableAcceptColumn2(t *testing.T) {
	filters := make(map[string]interface{})
	aViper := viper.New()
	aViper.SetConfigType("JSON")
	aViper.ReadConfig(bytes.NewBuffer([]byte(`{"filter" :{"test":{"EMPLOYEE":["EMP_ID"]}}}`)))
	aViper.UnmarshalKey("filter", &filters)

	filter := &Filter{
		FilterPolicy: "accept",
		Filter:       filters,
	}
	if filter.IsFilteredTable("test2", "EMPLOYEE") {
		t.Fail()
	}
}

func TestTableAcceptColumn3(t *testing.T) {
	filters := make(map[string]interface{})
	aViper := viper.New()
	aViper.SetConfigType("JSON")
	aViper.ReadConfig(bytes.NewBuffer([]byte(`{"filter" :{"test":{"EMPLOYEE":["EMP_ID"]}}}`)))
	aViper.UnmarshalKey("filter", &filters)

	filter := &Filter{
		FilterPolicy: "accept",
		Filter:       filters,
	}
	if filter.IsFilteredTable("test", "EMPLOYEE2") {
		t.Fail()
	}
}

func TestTableAcceptColumn4(t *testing.T) {
	filters := make(map[string]interface{})
	aViper := viper.New()
	aViper.SetConfigType("JSON")
	aViper.ReadConfig(bytes.NewBuffer([]byte(`{"filter" :{"test":{"EMPLOYEE":["EMP_ID"]}}}`)))
	aViper.UnmarshalKey("filter", &filters)

	filter := &Filter{
		FilterPolicy: "accept",
		Filter:       filters,
	}
	if filter.IsFilteredTable("test2", "EMPLOYEE2") {
		t.Fail()
	}
}

func TestColumnDrop(t *testing.T) {
	filters := make(map[string]interface{})
	aViper := viper.New()
	aViper.SetConfigType("JSON")
	aViper.ReadConfig(bytes.NewBuffer([]byte(`{"filter" :{"test":{"EMPLOYEE":["EMP_ID"]}}}`)))

	aViper.UnmarshalKey("filter", &filters)
	filter := &Filter{
		FilterPolicy: "drop",
		Filter:       filters,
	}
	if filter.IsFilteredColumn("test", "EMPLOYEE", "EMP_ID") {
		t.Fail()
	}
}

func TestColumnDrop2(t *testing.T) {
	filters := make(map[string]interface{})
	aViper := viper.New()
	aViper.SetConfigType("JSON")
	aViper.ReadConfig(bytes.NewBuffer([]byte(`{"filter" :{"test":{"EMPLOYEE":["EMP_ID"]}}}`)))

	aViper.UnmarshalKey("filter", &filters)
	filter := &Filter{
		FilterPolicy: "drop",
		Filter:       filters,
	}
	if !filter.IsFilteredColumn("test2", "EMPLOYEE", "EMP_ID") {
		t.Fail()
	}
}

func TestColumnDrop3(t *testing.T) {
	filters := make(map[string]interface{})
	aViper := viper.New()
	aViper.SetConfigType("JSON")
	aViper.ReadConfig(bytes.NewBuffer([]byte(`{"filter" :{"test":{"EMPLOYEE":["EMP_ID"]}}}`)))

	aViper.UnmarshalKey("filter", &filters)
	filter := &Filter{
		FilterPolicy: "drop",
		Filter:       filters,
	}
	if !filter.IsFilteredColumn("test", "EMPLOYEE2", "EMP_ID") {
		t.Fail()
	}
}

func TestColumnDrop4(t *testing.T) {
	filters := make(map[string]interface{})
	aViper := viper.New()
	aViper.SetConfigType("JSON")
	aViper.ReadConfig(bytes.NewBuffer([]byte(`{"filter" :{"test":{"EMPLOYEE":["EMP_ID"]}}}`)))

	aViper.UnmarshalKey("filter", &filters)
	filter := &Filter{
		FilterPolicy: "drop",
		Filter:       filters,
	}
	if !filter.IsFilteredColumn("test", "EMPLOYEE", "EMP_ID2") {
		t.Fail()
	}
}

func TestColumnDrop5(t *testing.T) {
	filters := make(map[string]interface{})
	aViper := viper.New()
	aViper.SetConfigType("JSON")
	aViper.ReadConfig(bytes.NewBuffer([]byte(`{"filter" :{"test":{"EMPLOYEE":["EMP_ID"]}}}`)))

	aViper.UnmarshalKey("filter", &filters)
	filter := &Filter{
		FilterPolicy: "drop",
		Filter:       filters,
	}
	if !filter.IsFilteredColumn("test2", "EMPLOYEE2", "EMP_ID2") {
		t.Fail()
	}
}

func TestColumnAccept(t *testing.T) {
	filters := make(map[string]interface{})
	aViper := viper.New()
	aViper.SetConfigType("JSON")
	aViper.ReadConfig(bytes.NewBuffer([]byte(`{"filter" :{"test":{"EMPLOYEE":["EMP_ID"]}}}`)))

	aViper.UnmarshalKey("filter", &filters)
	filter := &Filter{
		FilterPolicy: "accept",
		Filter:       filters,
	}
	if !filter.IsFilteredColumn("test", "EMPLOYEE", "EMP_ID") {
		t.Fail()
	}
}

func TestColumnAccept2(t *testing.T) {
	filters := make(map[string]interface{})
	aViper := viper.New()
	aViper.SetConfigType("JSON")
	aViper.ReadConfig(bytes.NewBuffer([]byte(`{"filter" :{"test":{"EMPLOYEE":["EMP_ID"]}}}`)))

	aViper.UnmarshalKey("filter", &filters)
	filter := &Filter{
		FilterPolicy: "accept",
		Filter:       filters,
	}
	if filter.IsFilteredColumn("test2", "EMPLOYEE", "EMP_ID") {
		t.Fail()
	}
}

func TestColumnAccept3(t *testing.T) {
	filters := make(map[string]interface{})
	aViper := viper.New()
	aViper.SetConfigType("JSON")
	aViper.ReadConfig(bytes.NewBuffer([]byte(`{"filter" :{"test":{"EMPLOYEE":["EMP_ID"]}}}`)))

	aViper.UnmarshalKey("filter", &filters)
	filter := &Filter{
		FilterPolicy: "accept",
		Filter:       filters,
	}
	if filter.IsFilteredColumn("test", "EMPLOYEE2", "EMP_ID") {
		t.Fail()
	}
}

func TestColumnAccept4(t *testing.T) {
	filters := make(map[string]interface{})
	aViper := viper.New()
	aViper.SetConfigType("JSON")
	aViper.ReadConfig(bytes.NewBuffer([]byte(`{"filter" :{"test":{"EMPLOYEE":["EMP_ID"]}}}`)))

	aViper.UnmarshalKey("filter", &filters)
	filter := &Filter{
		FilterPolicy: "accept",
		Filter:       filters,
	}
	if filter.IsFilteredColumn("test", "EMPLOYEE", "EMP_ID2") {
		t.Fail()
	}
}

func TestColumnAccept5(t *testing.T) {
	filters := make(map[string]interface{})
	aViper := viper.New()
	aViper.SetConfigType("JSON")
	aViper.ReadConfig(bytes.NewBuffer([]byte(`{"filter" :{"test":{"EMPLOYEE":["EMP_ID"]}}}`)))

	aViper.UnmarshalKey("filter", &filters)
	filter := &Filter{
		FilterPolicy: "accept",
		Filter:       filters,
	}
	if filter.IsFilteredColumn("test2", "EMPLOYEE2", "EMP_ID2") {
		t.Fail()
	}
}
