package jz

import (
	"errors"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"github.com/jonnywang/go-kits/redis"
	"strings"
)

var (
	ERR_PARAMS = errors.New("error params")
	ERR_TARGET_HOST = errors.New("error target host")
	NOT_FOUND_FILES = errors.New("not found rsync files")
	NOT_TRANSFER_FILE_MD5SUM = errors.New("error transfer file md5sum")
)

const (
	VERSION = "0.0.1"
)

type JzRsyncRedisHandle struct {
	redis.RedisHandler
	sync.Mutex
	rsync *JzRsync
	pullSig chan bool
}

func (obj *JzRsyncRedisHandle) Init() error {
	obj.Lock()
	defer obj.Unlock()

	obj.pullSig = make(chan bool)

	obj.Initiation(nil)

	obj.rsync = &JzRsync{}
	obj.rsync.Init()

	go func() {
		obj.rsync.Run(obj.pullSig)
	}()

	return nil
}

func (obj *JzRsyncRedisHandle) Shutdown() {
	redis.Logger.Print("searcher server will shutdown!!!")
	obj.rsync.Stop()
}

func (obj *JzRsyncRedisHandle) Version() (string, error) {
	return VERSION, nil
}

func (obj *JzRsyncRedisHandle) Sync() (error) {
	go func() {
		obj.pullSig <- true
	}()
	return nil
}

func (obj *JzRsyncRedisHandle) Setex(hostName, file, md5sum string) (error) {
	return obj.Set(hostName, file, "EX", md5sum)
}

func (obj *JzRsyncRedisHandle) Set(hostName, file, action, md5sum string) (error) {
	if len(hostName) == 0 || len(file) == 0 {
		return ERR_PARAMS
	}

	if len(action) > 0 {
		if strings.ToLower(action) != "ex" {
			return ERR_PARAMS
		}

		if len(md5sum) != 32 {
			return ERR_PARAMS
		}
	}

	hostNames := strings.Split(strings.ToUpper(hostName), ",")
	if false == InStringArray("*", hostNames) && false == HasIntersection(hostNames, obj.rsync.AllTargetHostNames) {
		return ERR_TARGET_HOST
	}

	task, err := AssembleTask(0, file)
	if err != nil || task.Size == 0 {
		return NOT_FOUND_FILES
	}

	if len(md5sum) > 0 && strings.ToLower(md5sum) != task.M5Sum {
		return NOT_TRANSFER_FILE_MD5SUM
	}

	task.HostNames = append(task.HostNames, hostNames...)

	obj.rsync.Send(task)

	return nil
}

func Run() {
	redis.Logger.Print(jzRsyncConfig)

	jzRsyncRedisHandle := &JzRsyncRedisHandle{}

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

