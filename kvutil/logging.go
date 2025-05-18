package kvutil

import (
	"io"
	"log"
	"os"
)

var (
	RaftLogger        = newLogger(os.Stdout, "RAFT ", log.LstdFlags|log.Lmicroseconds)
	ShardKVLogger     = newLogger(os.Stdout, "SHARDKV ", log.LstdFlags|log.Lmicroseconds)
	ShardCtrlerLogger = newLogger(os.Stdout, "SHARDCTRLER ", log.LstdFlags|log.Lmicroseconds)
)

func newLogger(w io.Writer, prefix string, flags int) *log.Logger {
	return log.New(w, prefix, flags)
}
