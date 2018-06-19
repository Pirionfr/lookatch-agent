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

func NewFSContext(dir, prefix string) *FSContext {
	return &FSContext{
		dir:    dir,
		prefix: prefix,
	}
}

func (ctx *FSContext) Store(name, value string) error {
	err := ioutil.WriteFile(ctx.filename(name), []byte(value), 0666)
	return errors.Annotate(err, "error writting file")
}

func (ctx *FSContext) Load(name string) (string, error) {
	data, err := ioutil.ReadFile(ctx.filename(name))
	return string(data), errors.Annotate(err, "error reading file")
}

func (ctx *FSContext) filename(name string) string {
	return path.Join(ctx.dir, ctx.prefix+"_"+name)
}
