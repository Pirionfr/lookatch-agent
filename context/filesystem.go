package context

import (
	"io/ioutil"
	"path"

	"github.com/juju/errors"
)

// FSContext is context using the local filesystem
type FSContext struct {
	dir    string
	prefix string
}

// NewFSContext create a new filesystem context
func NewFSContext(dir, prefix string) *FSContext {
	return &FSContext{
		dir:    dir,
		prefix: prefix,
	}
}

// Store write a key/value in filesystem context
func (ctx *FSContext) Store(name, value string) error {
	err := ioutil.WriteFile(ctx.filename(name), []byte(value), 0666)
	return errors.Annotate(err, "error writing file")
}

// Load read a key/value from filesystem context
func (ctx *FSContext) Load(name string) (string, error) {
	data, err := ioutil.ReadFile(ctx.filename(name))
	return string(data), errors.Annotate(err, "error reading file")
}

// filename returns the filename with prefix
func (ctx *FSContext) filename(name string) string {
	return path.Join(ctx.dir, ctx.prefix+"_"+name)
}
