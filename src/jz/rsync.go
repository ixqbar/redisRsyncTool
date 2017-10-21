package jz

import (
	"sync"
	"net"
	"fmt"
	"strings"
	"time"
	"os"
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

func (this *JzRsyncTarget) Connect() (error) {
	conn, err := net.Dial("tcp", this.Target.Address)
	if err != nil {
		JzLogger.Printf("connecting target server %s failed %s", this.Target.Name, err)
		return err
	}

	JzLogger.Printf("connecting target server %s success", this.Target.Name)

	this.conn = conn
	this.tryConnect = false

	return nil;
}

func (this *JzRsyncTarget) Start() {
	this.buffer = make([]byte, 1024)
	this.tryConnect = true
	this.connStopped = make(chan bool, 1)
	this.stopped = make(chan bool, 1)

	go func() {
		interval := time.NewTicker(time.Second * time.Duration(5))
		defer interval.Stop()

	T:
		for {
			select {
			case <- this.connStopped:
				JzLogger.Print("catch TargetServerStopped signal")
				break T
			case <- interval.C:
				func() {
					this.Lock()
					defer this.Unlock()

					if this.tryConnect {
						err := this.Connect()
						if err != nil {
							JzLogger.Printf("reconnect target server %s failed %s", this.Target.Name, err)
							return
						}
						this.tryConnect = false
					}

					this.conn.Write([]byte("PING\r\n"))
					n, err := this.conn.Read(this.buffer)
					if err == nil {
						JzLogger.Printf("ping %s got %s", this.Target.Name, strings.Trim(string(this.buffer[:n]), "\r\n"))
					}
				}()
			}
		}

		this.stopped <- true
	}()
}

func (this *JzRsyncTarget) Stop() {
	JzLogger.Printf("send stopped signal to %s", this.Target.Name)
	this.connStopped <- true
	<-this.stopped
	JzLogger.Printf("%s stopped", this.Target.Name)
}

func (this *JzRsyncTarget) Rsync(t *JzTask) (bool, error) {
	this.Lock()
	defer this.Unlock()

	if this.tryConnect {
		err := this.Connect()
		if err != nil {
			JzLogger.Print("reconnect target server %s failed %s", this.Target.Name, err)
			return false, err
		}
		this.tryConnect = false
	}

	f, err := os.Open(t.Path)
	if err != nil {
		JzLogger.Print("Open file %s failed %v", t.AbsolutePath, err)
		return false, err
	}
	defer f.Close();

	targetFileSuffix := fmt.Sprintf("%s@%d@%s@%s\r\n", t.Name, t.Size, t.M5Sum, t.RelativePath)
	this.conn.Write([]byte(targetFileSuffix))

	buf := make([]byte, 1024)
	total := 0
	for {
		nr, er := f.Read(buf)
		if er != nil {
			break
		}

		if nr > 0 {
			nw, ew := this.conn.Write(buf[0:nr])
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
	n, err := this.conn.Read(this.buffer)
	if err == nil {
		rr := strings.Trim(string(this.buffer[:n]), "\r\n")
		if rr == "OK" {
			r = true
		}
		JzLogger.Printf("Transerf %s to %s %s", t.Path, this.Target.Name, rr)
	} else {
		this.tryConnect = true
		JzLogger.Printf("Transerf %s to %s failed %s", t.Path,  this.Target.Name, err)
	}

	return r, err
}

type JzRsync struct {
	stopped chan bool
	taskToStopped chan bool
	intervalToStopped chan bool
	queue chan *JzTask
	target []*JzRsyncTarget
	AllTargetNames []string
}

func (this *JzRsync) Init()  {
	this.stopped = make(chan bool, 2)
	this.taskToStopped = make(chan bool, 1)
	this.intervalToStopped = make(chan bool, 1)
	this.queue = make(chan *JzTask, 100)
	this.AllTargetNames = make([]string, len(jzRsyncConfig.TargetServer))

	this.target = make([]*JzRsyncTarget, 0)
	for _, s := range jzRsyncConfig.TargetServer {
		t := &JzRsyncTarget{Target:&s}
		t.Start()

		this.target = append(this.target, t)
		this.AllTargetNames = append(this.AllTargetNames, strings.ToUpper(s.Name))
	}
}

func (this *JzRsync) Send(t *JzTask) (bool, error) {
	this.queue <- t
	return true, nil
}

func (this *JzRsync) Stop() {
	JzLogger.Print("send Stopped signal")
	this.intervalToStopped <- true
	this.taskToStopped <- true
	JzLogger.Print("send Stopped signal OK")

	<- this.stopped
	<- this.stopped

	for _, ts := range this.target {
		ts.Stop()
	}

	JzLogger.Print("rsync stopped")
}

func (this *JzRsync) Interval() {
	interval := time.NewTicker(time.Second * time.Duration(jzRsyncConfig.Interval))
	defer interval.Stop()

F:
	for {
		select {
			case <- this.intervalToStopped:
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
						this.queue <- t
					}
				}
			}()
		}
	}

	this.stopped <- true
	JzLogger.Print("interval exit")
}

func (this *JzRsync) Run() {
	if jzRsyncConfig.Interval > 0 {
		go func() {
			this.Interval()
		}()
	} else {
		this.stopped <- true
	}

E:
	for {
		select {
			case <- this.taskToStopped:
				JzLogger.Println("catch taskToStopped signal")
				break E
			case t:= <- this.queue:
				JzLogger.Print("get task from queue", t)
				n := 0
				for _, ts := range this.target {
					if len(t.Scope) != 0 && InStringArray(strings.ToUpper(ts.Target.Name), t.Scope) == false && InStringArray("ALL", t.Scope) == false {
						n += 1
						continue
					}
					ok, err := ts.Rsync(t)
					if !ok {
						JzLogger.Print(err)
						continue
					}
					n += 1
				}

				t.Done(n)
		}
	}

	this.stopped <- true
	JzLogger.Print("rsync exit")
}