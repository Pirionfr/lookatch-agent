package util

import (
	"reflect"
	"testing"

	"github.com/spf13/viper"
)

func TestIsNotStandalone(t *testing.T) {
	v := viper.New()
	v.Set("auth", "test")
	if IsStandalone(v) {
		t.Fail()
	}
}

func TestIsStandalone(t *testing.T) {
	v := viper.New()
	v.Set("auth", nil)
	if !IsStandalone(v) {
		t.Fail()
	}
}

func TestIsStandalone2(t *testing.T) {
	v := viper.New()
	if !IsStandalone(v) {
		t.Fail()
	}
}

func TestEscapeCtrl(t *testing.T) {
	aString := "test"
	res := EscapeCtrl([]byte(aString))
	if string(res) != aString {
		t.Fail()
	}
}

func TestEscapeCtrl2(t *testing.T) {
	aString := []byte{0, 116, 101, 115, 116}
	res := EscapeCtrl([]byte(aString))

	expected := []byte{92, 117, 48, 48, 48, 48, 116, 101, 115, 116}
	if !reflect.DeepEqual(expected, res) {
		t.Fail()
	}

}
