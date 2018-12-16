package context

import (
	"testing"

	"github.com/alicebob/miniredis"
)

// TestContextFileSystemType test new file
func TestContextFileSystemType(t *testing.T) {
	ctx, err := NewContext("file://./test")
	if err != nil {
		t.Fail()
	}

	_, ok := ctx.(*FSContext)
	if !ok {
		t.Fail()
	}
}

// TestContextRedisType test new redis
func TestContextRedisType(t *testing.T) {
	s, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	defer s.Close()

	ctx, err := NewContext("redis://" + s.Addr())
	if err != nil {
		t.Fail()
	}

	_, ok := ctx.(*RedisContext)
	if !ok {
		t.Fail()
	}
	s.Close()
}
