package utils

import (
	"reflect"
	"testing"

	"github.com/spf13/viper"
)

func TestIsNotStandalone(t *testing.T) {
	v := viper.New()
	v.Set("controller", "test")
	if IsStandalone(v) {
		t.Fail()
	}
}

func TestIsStandalone(t *testing.T) {
	v := viper.New()
	v.Set("controller", nil)
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
	aByteArray := []byte{0, 116, 101, 115, 116}
	res := EscapeCtrl(aByteArray)

	expected := []byte{92, 117, 48, 48, 48, 48, 116, 101, 115, 116}
	if !reflect.DeepEqual(expected, res) {
		t.Fail()
	}

}

func TestInSlice(t *testing.T) {
	StringSlice := []string{
		"foo",
		"bar",
	}

	if !InSlice(StringSlice, "foo") {
		t.Fail()
	}

}

func TestInSlice2(t *testing.T) {
	StringSlice := []string{
		"foo",
		"bar",
	}

	if InSlice(StringSlice, "foobar") {
		t.Fail()
	}

}

func TestNumDot(t *testing.T) {
	StringMap := map[string]bool{
		"254":  false,
		"32.2": true,
		"for":  false,
	}
	for k, v := range StringMap {
		if v != IsNumDot(k) {
			t.Fail()
		}
	}

}
