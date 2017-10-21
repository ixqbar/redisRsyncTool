package jz

import (
	"errors"
	"os"
	"os/signal"
	"sync"
	"syscall"
	redis "github.com/jonnywang/go-kits/redis"
	"strings"
)

var (
	ERR_PARAMS = errors.New("error params")
	ERR_TARGET_HOST = errors.New("error target host")
	NOT_FOUND_FIELS = errors.New("not found rsync files")
	NOT_TRANSFER_FILE_MD5SUM = errors.New("error transfer file md5sum")
)

const (
	VERSION = "0.0.1"
	OK      = "OK"
)

type JzRsyncRedisHandle struct {
	redis.RedisHandler
	sync.Mutex
	rsync *JzRsync
}

func (this *JzRsyncRedisHandle) Init() error {
	this.Lock()
	defer this.Unlock()

	this.rsync = &JzRsync{}
	this.rsync.Init()

	go func() {
		this.rsync.Run()
	}()

	return nil
}

func (this *JzRsyncRedisHandle) Shutdown() {
	redis.Logger.Print("searcher server will shutdown!!!")
	this.rsync.Stop()
}

func (this *JzRsyncRedisHandle) Version() (string, error) {
	return VERSION, nil
}

func (this *JzRsyncRedisHandle) Setex(host, name, md5sum string) (string, error) {
	return this.Set(host, name, "EX", md5sum)
}

func (this *JzRsyncRedisHandle) Set(host, name, action, md5sum string) (string, error) {
	if len(host) == 0 || len(name) == 0 {
		return "", ERR_PARAMS
	}

	if len(action) > 0 {
		if strings.ToLower(action) != "ex" {
			return "", ERR_PARAMS
		}

		if len(md5sum) != 32 {
			return "", ERR_PARAMS
		}
	}

	host = strings.ToUpper(host)
	if host != "ALL" && false == InStringArray(host, this.rsync.AllTargetNames) {
		return "", ERR_TARGET_HOST
	}

	task, err := AssembleTask(0, name)
	if err != nil || task.Size == 0 {
		return "", NOT_FOUND_FIELS
	}

	if len(md5sum) > 0 && strings.ToLower(md5sum) != task.M5Sum {
		return "",NOT_TRANSFER_FILE_MD5SUM
	}

	task.Scope = append(task.Scope, host)

	this.rsync.Send(task)

	return OK, nil
}

func Run() {
	redis.Logger.Print(jzRsyncConfig)

	jzRsyncRedisHandle := &JzRsyncRedisHandle{}

	jzRsyncRedisHandle.SetShield("Init")
	jzRsyncRedisHandle.SetShield("Shutdown")
	jzRsyncRedisHandle.SetShield("Lock")
	jzRsyncRedisHandle.SetShield("Unlock")
	jzRsyncRedisHandle.SetShield("SetShield")
	jzRsyncRedisHandle.SetShield("SetConfig")
	jzRsyncRedisHandle.SetShield("CheckShield")

	defer func() {
		jzRsyncRedisHandle.Shutdown()
	}()

	err := jzRsyncRedisHandle.Init()
	if err != nil {
		JzLogger.Print(err)
		return
	}

	server, err := redis.NewServer(jzRsyncConfig.Address, jzRsyncRedisHandle)
	if err != nil {
		JzLogger.Print(err)
		return
	}

	sigs := make(chan os.Signal)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		<-sigs
		server.Stop(10)
	}()

	redis.Logger.Printf("server run at %s", jzRsyncConfig.Address)

	err = server.Start()
	if err != nil {
		JzLogger.Print(err)
	}
}

