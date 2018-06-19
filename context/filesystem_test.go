package context

import (
	"os"
	"path"
	"testing"
)

func TestContextFileSystem(t *testing.T) {
	testValue := "test_value"
	dir := "./"
	prefixFile := "test_"
	suffixFile := "test1"

	ctx := NewFSContext(dir, prefixFile)
	ctx.Store(suffixFile, testValue)
	value, err := ctx.Load("test1")
	if err != nil || value != testValue {
		t.Error(err)
	}

	os.Remove(path.Join(ctx.dir, ctx.prefix+"_"+suffixFile))

}
