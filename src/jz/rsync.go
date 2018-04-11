package jz

import (
	"sync"
	"net"
	"fmt"
	"strings"
	"time"
	"os"
	"errors"
	"io"
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
		JzLogger.Printf("connecting target server %s[%s] failed %s", obj.Target.Name, obj.Target.Address, err)
		return err
	}

	JzLogger.Printf("connecting target server %s[%s] success", obj.Target.Name, obj.Target.Address)

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
							JzLogger.Printf("reconnect target server %s[%s] failed %s", obj.Target.Name, obj.Target.Address, err)
							return
						}
						obj.tryConnect = false
					}

					obj.conn.Write([]byte("PING\r\n"))
					n, err := obj.conn.Read(obj.buffer)
					if err == nil {
						JzLogger.Printf("ping %s[%s] got %s", obj.Target.Name, obj.Target.Address, strings.Trim(string(obj.buffer[:n]), "\r\n"))
					}
				}()
			}
		}

		obj.stopped <- true
	}()
}

func (obj *JzRsyncTarget) Stop() {
	JzLogger.Printf("send stopped signal to %s[%s]", obj.Target.Name, obj.Target.Address)
	obj.connStopped <- true
	<-obj.stopped
	JzLogger.Printf("%s[%s] stopped", obj.Target.Name, obj.Target.Address)
}

func (obj *JzRsyncTarget) Rsync(t *JzTask, num int) (bool, error) {
	loop := 0

	for {
		if loop > num {
			return false, errors.New(fmt.Sprintf("rsync %s to server %s[%s] failed", t.Path, obj.Target.Name, obj.Target.Address))
		}

		loop++
		ok, err := obj.RsyncOnce(t)
		if err != nil {
			if err != io.EOF {
				JzLogger.Print(err)
			}
			continue
		}

		if !ok {
			JzLogger.Printf("try again rsync %s to server %s[%s]", t.Path, obj.Target.Name, obj.Target.Address)
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
			JzLogger.Printf("reconnect target server %s[%s] failed %s", obj.Target.Name, obj.Target.Address, err)
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
		JzLogger.Printf("Transerf %s to %s[%s] %s", t.Path, obj.Target.Name, obj.Target.Address, rr)
	} else {
		obj.tryConnect = true
		JzLogger.Printf("Transerf %s to %s[%s] failed %s", t.Path, obj.Target.Name, obj.Target.Address, err)
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
		obj.AllTargetHostNames = append(obj.AllTargetHostNames, jzRsyncConfig.TargetServer[i].Group...)
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

func (obj *JzRsync) pullTasks()  {
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
}

func (obj *JzRsync) Run(newTask chan bool) {
	if jzRsyncConfig.Interval > 0 {
		go func() {
			interval := time.NewTicker(time.Second * time.Duration(jzRsyncConfig.Interval))
			defer interval.Stop()

		F:
			for {
				select {
				case <- obj.intervalToStopped:
					JzLogger.Print("catch intervalStopped signal")
					break F
				case <- interval.C:
					obj.pullTasks()
				case <- newTask:
					obj.pullTasks()
				}
			}

			obj.stopped <- true
			JzLogger.Print("interval exit")
		}()
	} else {
		obj.stopped <- true
	}

E:
	for {
		select {
			case <- obj.taskToStopped:
				JzLogger.Print("catch taskToStopped signal")
				break E
			case task:= <- obj.queue:
				JzLogger.Print("get task from queue", task)
				n := 0
				for _, hn := range task.HostNames {
					for _, ts := range obj.target {
						JzLogger.Printf("task id %d-%s will rsync for %s[%s]", task.Id, hn, ts.Target.Name, ts.Target.Address)
						if  hn != "*" && InStringArray(hn, ts.Target.Group) == false {
							n += 1
							JzLogger.Printf("task id %d-%s rsync ignore for %s[%s]", task.Id, hn, ts.Target.Name, ts.Target.Address)
							continue
						}

						ok, err := ts.Rsync(task, task.RsyncMaxNum)
						if !ok {
							JzLogger.Print(err)
							continue
						}

						JzLogger.Printf("task id %d-%s rsync success for %s[%s]", task.Id, hn, ts.Target.Name, ts.Target.Address)

						n += 1
					}
				}
				task.Done(n)
		}
	}

	obj.stopped <- true
	JzLogger.Print("rsync exit")
}