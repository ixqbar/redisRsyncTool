package jz

import (
	"sync"
	"net"
	"fmt"
	"strings"
	"time"
	"os"
	"errors"
)

type JzRsyncTarget struct {
	sync.Mutex
	Target *JzTargetServer
	conn net.Conn
	buffer []byte
	tryConnect bool
	connStopped chan bool
	stopped chan bool
}

func (obj *JzRsyncTarget) Connect() (error) {
	conn, err := net.Dial("tcp", obj.Target.Address)
	if err != nil {
		JzLogger.Printf("connecting target server %s failed %s", obj.Target.Address, err)
		return err
	}

	JzLogger.Printf("connecting target server %s run at %s success", obj.Target.Name, obj.Target.Address)

	obj.conn = conn
	obj.tryConnect = false

	return nil
}

func (obj *JzRsyncTarget) Start() {
	obj.buffer = make([]byte, 1024)
	obj.tryConnect = true
	obj.connStopped = make(chan bool, 1)
	obj.stopped = make(chan bool, 1)

	go func() {
		interval := time.NewTicker(time.Second * time.Duration(5))
		defer interval.Stop()

	T:
		for {
			select {
			case <- obj.connStopped:
				JzLogger.Print("catch TargetServerStopped signal")
				break T
			case <- interval.C:
				func() {
					obj.Lock()
					defer obj.Unlock()

					if obj.tryConnect {
						err := obj.Connect()
						if err != nil {
							JzLogger.Printf("reconnect target server %s failed %s", obj.Target.Name, err)
							return
						}
						obj.tryConnect = false
					}

					obj.conn.Write([]byte("PING\r\n"))
					n, err := obj.conn.Read(obj.buffer)
					if err == nil {
						JzLogger.Printf("ping %s got %s", obj.Target.Name, strings.Trim(string(obj.buffer[:n]), "\r\n"))
					}
				}()
			}
		}

		obj.stopped <- true
	}()
}

func (obj *JzRsyncTarget) Stop() {
	JzLogger.Printf("send stopped signal to %s", obj.Target.Name)
	obj.connStopped <- true
	<-obj.stopped
	JzLogger.Printf("%s stopped", obj.Target.Name)
}

func (obj *JzRsyncTarget) Rsync(t *JzTask, num int) (bool, error) {
	loop := 0

	for {
		if loop > num {
			return false, errors.New(fmt.Sprintf("rsync %s to server %s failed", t.Path, obj.Target.Address))
		}

		loop++
		ok, err := obj.RsyncOnce(t)
		if err != nil {
			JzLogger.Print(err)
			continue
		}

		if !ok {
			JzLogger.Printf("try again rsync %s to server %s", t.Path, obj.Target.Address)
			continue
		}

		return true,nil
	}
}

func (obj *JzRsyncTarget) RsyncOnce(t *JzTask) (bool, error) {
	obj.Lock()
	defer obj.Unlock()

	if obj.tryConnect {
		err := obj.Connect()
		if err != nil {
			JzLogger.Printf("reconnect target server %s failed %s", obj.Target.Name, err)
			return false, err
		}
		obj.tryConnect = false
	}

	f, err := os.Open(t.Path)
	if err != nil {
		JzLogger.Printf("Open file %s failed %v", t.AbsolutePath, err)
		return false, err
	}
	defer f.Close()

	targetFileSuffix := fmt.Sprintf("%s@%d@%s@%s\r\n", t.Name, t.Size, t.M5Sum, t.RelativePath)
	obj.conn.Write([]byte(targetFileSuffix))

	buf := make([]byte, 1024)
	total := 0
	for {
		nr, er := f.Read(buf)
		if er != nil {
			break
		}

		if nr > 0 {
			nw, ew := obj.conn.Write(buf[0:nr])
			if ew != nil {
				break
			}

			total += nw
		}

		if int64(total) >= t.Size {
			break
		}
	}

	r := false
	n, err := obj.conn.Read(obj.buffer)
	if err == nil {
		rr := strings.Trim(string(obj.buffer[:n]), "\r\n")
		if rr == "OK" {
			r = true
		}
		JzLogger.Printf("Transerf %s to %s %s", t.Path, obj.Target.Name, rr)
	} else {
		obj.tryConnect = true
		JzLogger.Printf("Transerf %s to %s failed %s", t.Path,  obj.Target.Name, err)
	}

	return r, err
}

type JzRsync struct {
	stopped chan bool
	taskToStopped chan bool
	intervalToStopped chan bool
	queue chan *JzTask
	target []*JzRsyncTarget
	AllTargetHostNames []string
}

func (obj *JzRsync) Init()  {
	obj.stopped = make(chan bool, 2)
	obj.taskToStopped = make(chan bool, 1)
	obj.intervalToStopped = make(chan bool, 1)
	obj.queue = make(chan *JzTask, 100)
	obj.AllTargetHostNames = make([]string, len(jzRsyncConfig.TargetServer))

	t := len(jzRsyncConfig.TargetServer)
	obj.target = make([]*JzRsyncTarget, t)
	for i := 0; i < t ; i++ {
		obj.target[i] = &JzRsyncTarget{Target:&jzRsyncConfig.TargetServer[i]}
		obj.target[i].Start()
		obj.AllTargetHostNames = append(obj.AllTargetHostNames, strings.ToUpper(jzRsyncConfig.TargetServer[i].Name))
	}
}

func (obj *JzRsync) Send(t *JzTask) (bool, error) {
	obj.queue <- t
	return true, nil
}

func (obj *JzRsync) Stop() {
	JzLogger.Print("send Stopped signal")
	obj.intervalToStopped <- true
	obj.taskToStopped <- true
	JzLogger.Print("send Stopped signal OK")

	<- obj.stopped
	<- obj.stopped

	for _, ts := range obj.target {
		ts.Stop()
	}

	JzLogger.Print("rsync stopped")
}

func (obj *JzRsync) Interval() {
	interval := time.NewTicker(time.Second * time.Duration(jzRsyncConfig.Interval))
	defer interval.Stop()

F:
	for {
		select {
			case <- obj.intervalToStopped:
				JzLogger.Print("catch intervalStopped signal")
				break F
			case <- interval.C:
			func() {
				tasks, err := JzDaoInstance().GetTasks()
				if err != nil {
					JzLogger.Print(err)
					return
				}

				if len(tasks) > 0 {
					for _, t := range tasks {
						obj.queue <- t
					}
				}
			}()
		}
	}

	obj.stopped <- true
	JzLogger.Print("interval exit")
}

func (obj *JzRsync) Run() {
	if jzRsyncConfig.Interval > 0 {
		go func() {
			obj.Interval()
		}()
	} else {
		obj.stopped <- true
	}

E:
	for {
		select {
			case <- obj.taskToStopped:
				JzLogger.Println("catch taskToStopped signal")
				break E
			case t:= <- obj.queue:
				JzLogger.Print("get task from queue", t)
				n := 0
				for _, ts := range obj.target {
					JzLogger.Printf("task id %d will rsync for %s run at %s", t.Id, ts.Target.Name, ts.Target.Address)
					if InStringArray(strings.ToUpper(ts.Target.Name), t.HostNames) == false && InStringArray("ALL", t.HostNames) == false {
						n += 1
						JzLogger.Printf("task id %d ignore with %s run at %s", t.Id, ts.Target.Name, ts.Target.Address)
						continue
					}

					ok, err := ts.Rsync(t, t.RsyncMaxNum)
					if !ok {
						JzLogger.Print(err)
						break
					}

					n += 1
				}

				t.Done(n)
		}
	}

	obj.stopped <- true
	JzLogger.Print("rsync exit")
}