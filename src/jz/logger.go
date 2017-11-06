package jz

import (
	"log"
	"github.com/jonnywang/go-kits/redis"
)

func init()  {
	redis.Logger.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
}

var JzLogger = redis.Logger