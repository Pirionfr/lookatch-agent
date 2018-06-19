package context

import (
	"github.com/juju/errors"
	"github.com/mediocregopher/radix.v2/pool"
)

const (
	redisPoolSize                 = 4
	redisDefaultExpirationSeconds = 604800 // 1 week
)

type RedisContext struct {
	rdpool *pool.Pool
	prefix string
}

func NewRedisContext(addr, prefix string) (*RedisContext, error) {
	p, err := pool.New("tcp", addr, redisPoolSize)
	if err != nil {
		return nil, errors.Annotate(err, "failed to create redis")
	}
	return &RedisContext{
		rdpool: p,
		prefix: prefix,
	}, nil
}

func (ctx *RedisContext) Store(name, value string) (err error) {
	defer errors.DeferredAnnotatef(&err, "failed to store on redis")
	conn, err := ctx.rdpool.Get()
	if err != nil {
		return errors.Annotate(err, "error getting connection from pool")
	}
	defer ctx.rdpool.Put(conn)

	return conn.Cmd("SET", ctx.prefix+":"+name, value, "EX", redisDefaultExpirationSeconds).Err
}

func (ctx *RedisContext) Load(name string) (_ string, err error) {
	defer errors.DeferredAnnotatef(&err, "failed to load on redis")
	conn, err := ctx.rdpool.Get()
	if err != nil {
		return "", errors.Annotate(err, "error getting connection from pool")
	}
	defer ctx.rdpool.Put(conn)

	r := conn.Cmd("GET", ctx.prefix+":"+name)
	if r.Err != nil {
		return "", errors.Annotate(r.Err, "error running GET command on redis")
	}

	return r.Str()
}

func (ctx *RedisContext) Close() {
	ctx.rdpool.Empty()
}
