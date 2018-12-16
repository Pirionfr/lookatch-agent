package util

import (
	"testing"
)

var funcs = map[string]interface{}{
	"HelloWorld":  HelloWorld,
	"HelloWorld2": HelloWorld2,
}

/**
Reflect Call
*/
func HelloWorld() string {
	return "Hello World"
}

func HelloWorld2(name string) string {
	return "Hello " + name
}

func TestReflectWithoutParam(t *testing.T) {
	r, err := Call(funcs, "HelloWorld")
	if err != nil {
		t.Error(err)
	}

	if r != "Hello World" {
		t.Error("mistmatch")
	}
}

func TestReflectWithParam(t *testing.T) {
	r, err := Call(funcs, "HelloWorld2", "World")
	if err != nil {
		t.Error(err)
	}

	if r != "Hello World" {
		t.Error("mistmatch")
	}
}

func TestReflectWithBedParamNumber(t *testing.T) {
	_, err := Call(funcs, "HelloWorld2", "World", "Error")
	if err == nil {
		t.Fail()
	}

}
