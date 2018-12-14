package context

import (
	"testing"

	"github.com/alicebob/miniredis"
)

func TestContextRedis(t *testing.T) {
	s, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	defer s.Close()

	testValue := "test_value"

	prefixFile := "test_"
	suffixFile := "test1"

	ctx, err := NewRedisContext(s.Addr(), prefixFile)
	if err != nil {
		t.Error(err)
	}
	ctx.Store(suffixFile, testValue)
	value, err := ctx.Load("test1")
	if err != nil || value != testValue {
		t.Error(err)
	}
	ctx.Close()
}
