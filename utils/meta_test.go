package utils

import "testing"

func TestMewMeta(t *testing.T) {
	meta := NewMeta("test", "testvalue")

	if meta.Name != "test" {
		t.Fail()
	}

	if meta.Value != "testvalue" {
		t.Fail()
	}

	if meta.Timestamp == 0 {
		t.Fail()
	}
}

func TestMewMetas(t *testing.T) {
	metas := NewMetas()

	metas.SetMetaSources("default", NewMeta("test", "testvalue"))
	metas.SetMetaSinks("default", NewMeta("test", "testvalue"))

	if metas.Sources["default"]["test"].Name != "test" {
		t.Fail()
	}

	if metas.Sources["default"]["test"].Value != "testvalue" {
		t.Fail()
	}

	if metas.Sources["default"]["test"].Timestamp == 0 {
		t.Fail()
	}

	if metas.Sinks["default"]["test"].Name != "test" {
		t.Fail()
	}

	if metas.Sinks["default"]["test"].Value != "testvalue" {
		t.Fail()
	}

	if metas.Sinks["default"]["test"].Timestamp == 0 {
		t.Fail()
	}

}
