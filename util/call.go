package util

import (
	"errors"
	"reflect"
)

// Call reflect call
func Call(m map[string]interface{}, sourceType string, params ...interface{}) (result interface{}, err error) {
	f := reflect.ValueOf(m[sourceType])

	if len(params) != f.Type().NumIn() {
		err = errors.New("The number of params is not adapted")
		return
	}
	in := make([]reflect.Value, len(params))
	for k, param := range params {
		in[k] = reflect.ValueOf(param)
	}
	result = f.Call(in)[0].Interface()
	return
}
