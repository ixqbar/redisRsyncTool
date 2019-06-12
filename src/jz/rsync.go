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
	Target       *JzTargetServer
	conn         net.Conn
	localAddress string
	buffer       []byte
	tryConnect   bool
	connStopped  chan bool
	stopped      chan bool
	Name         string
}

func (obj *JzRsyncTarget) Connect() (error) {
	conn, err := net.Dial("tcp", obj.Target.Address)
	if err != nil {
		JzLogger.Printf("connecting target server %s[%s] failed %s", obj.Target.Name, obj.Target.Address, err)
		return err
	}

	obj.conn = conn
	obj.tryConnect = false
	obj.localAddress = obj.conn.LocalAddr().String()

	JzLogger.Printf("[%s]connecting target server %s[%s] success", obj.localAddress, obj.Target.Name, obj.Target.Address)

	return nil
}

func (obj *JzRsyncTarget) WriteAll(message []byte) bool {
	totalLen := len(message)
	writeLen := 0

	for {
		obj.conn.SetWriteDeadline(time.Now().Add(time.Second * time.Duration(30)))
		n, err := obj.conn.Write(message[writeLen:])
		if err != nil {
			JzLogger.Printf("[%s] write message %s target server %s[%s] failed %s", obj.localAddress, string(message), obj.Target.Name, obj.Target.Address, err)
			return false
		}

		writeLen += n
		if writeLen >= totalLen {
			return true
		}
	}
}

func (obj *JzRsyncTarget) ReadAll(minLen int) ([]byte, error) {
	readLen := 0

	for {
		obj.conn.SetWriteDeadline(time.Now().Add(time.Second * time.Duration(30)))
		n, err := obj.conn.Read(obj.buffer[readLen:])
		if err != nil {
			JzLogger.Printf("[%s] read target server %s[%s] response failed %s", obj.localAddress, obj.Target.Name, obj.Target.Address, err)
			break
		}

		readLen += n
		if readLen >= minLen {
			break
		}
	}

	return obj.buffer[:readLen], nil
}

func (obj *JzRsyncTarget) Start() {
	obj.buffer = make([]byte, 1024)
	obj.tryConnect = true
	obj.connStopped = make(chan bool, 1)
	obj.stopped = make(chan bool, 1)

	obj.Connect()

	go func() {
		interval := time.NewTicker(time.Second * time.Duration(5))
		defer interval.Stop()

	T:
		for {
			select {
			case <-obj.connStopped:
				JzLogger.Print("catch TargetServerStopped signal")
				break T
			case <-interval.C:
				func() {
					obj.Lock()
					defer obj.Unlock()

					if obj.tryConnect {
						err := obj.Connect()
						if err != nil {
							JzLogger.Printf("[%s]reconnect target server %s[%s] failed %s", obj.localAddress, obj.Target.Name, obj.Target.Address, err)
							return
						}
						obj.tryConnect = false
					}

					obj.WriteAll([]byte("PING\r\n"))

					message, err := obj.ReadAll(6)
					if err == nil && len(message) >= 6 {
						//JzLogger.Printf("[%s]ping %s[%s] got %s", obj.localAddress, obj.Target.Name, obj.Target.Address, strings.Trim(string(message), "\r\n"))
					} else {
						obj.tryConnect = true
						JzLogger.Printf("[%s]ping %s[%s] failed %v", obj.localAddress, obj.Target.Name, obj.Target.Address, err)
					}
				}()
			}
		}

		obj.stopped <- true
	}()
}

func (obj *JzRsyncTarget) Stop() {
	JzLogger.Printf("[%s]send stopped signal to %s[%s]", obj.localAddress, obj.Target.Name, obj.Target.Address)
	obj.connStopped <- true
	<-obj.stopped
	JzLogger.Printf("[%s]send stopped signal to %s[%s] success", obj.localAddress, obj.Target.Name, obj.Target.Address)
}

func (obj *JzRsyncTarget) Rsync(t *JzTask, num int) (bool, error) {
	loop := 0

	for {
		if loop > num {
			return false, errors.New(fmt.Sprintf("[%s]rsync %s to server %s[%s] failed", obj.localAddress, t.Path, obj.Target.Name, obj.Target.Address))
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
			JzLogger.Printf("[%s]try again rsync %s to server %s[%s]", obj.localAddress, t.Path, obj.Target.Name, obj.Target.Address)
			continue
		}

		return true, nil
	}
}

func (obj *JzRsyncTarget) RsyncOnce(t *JzTask) (bool, error) {
	obj.Lock()
	defer obj.Unlock()

	if obj.tryConnect {
		err := obj.Connect()
		if err != nil {
			JzLogger.Printf("[%s]reconnect target server %s[%s] failed %s", obj.localAddress, obj.Target.Name, obj.Target.Address, err)
			return false, err
		}
		obj.tryConnect = false
	}

	f, err := os.Open(t.Path)
	if err != nil {
		JzLogger.Printf("[%s]Open file %s failed %v", obj.localAddress, t.AbsolutePath, err)
		return false, err
	}
	defer f.Close()

	targetFileSuffix := fmt.Sprintf("%s@%d@%s@%s\r\n", t.Name, t.Size, t.M5Sum, t.RelativePath)
	obj.WriteAll([]byte(targetFileSuffix))

	//CONTINUE\r\n
	//ALL_SAME\r\n

	message, err := obj.ReadAll(10)
	if err == nil && len(message) >= 10 {
		rr := strings.Trim(string(message), "\r\n")
		if rr == "ALL_SAME" {
			JzLogger.Printf("[%s]Transfer %s to server %s[%s] success", obj.localAddress, t.Path, obj.Target.Name, obj.Target.Address)
			return true, nil
		}

		if rr != "CONTINUE" {
			obj.tryConnect = true
			JzLogger.Printf("[%s]Read Transfer %s to server %s[%s] header response [%s] failed %s", obj.localAddress, t.Path, obj.Target.Name, obj.Target.Address, rr, err)
			return false, errors.New("error transfer header response")
		}
	} else {
		obj.tryConnect = true
		JzLogger.Printf("[%s]Read Transfer %s to server %s[%s] header response failed %s", obj.localAddress, t.Path, obj.Target.Name, obj.Target.Address, err)
		return false, err
	}

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

	//OK\r\n
	message, err = obj.ReadAll(4)
	if err == nil && len(message) >= 4 {
		rr := strings.Trim(string(message), "\r\n")
		if rr == "OK" {
			JzLogger.Printf("[%s]Transfer %s to server %s[%s] success", obj.localAddress, t.Path, obj.Target.Name, obj.Target.Address)
			return true, nil
		}
	}

	obj.tryConnect = true
	JzLogger.Printf("[%s]Read Transfer %s to server %s[%s] failed %s", obj.localAddress, t.Path, obj.Target.Name, obj.Target.Address, err)

	return false, errors.New("error transfer header response")
}

type JzRsync struct {
	stopped           chan bool
	taskToStopped     chan bool
	intervalToStopped chan bool
	queue             chan *JzTask
	transferChannel   chan []*JzRsyncTarget
	allTargetServer   []*JzRsyncTarget
	AllTargetHostNames []string
}

func (obj *JzRsync) Init() {
	obj.stopped = make(chan bool, 2)
	obj.taskToStopped = make(chan bool, 1)
	obj.intervalToStopped = make(chan bool, 1)
	obj.queue = make(chan *JzTask, 100)

	transferTargetNumber := len(jzRsyncConfig.TargetServer)
	transferChannelNumber := transferTargetNumber * 10

	obj.transferChannel = make(chan []*JzRsyncTarget, transferChannelNumber)
	obj.AllTargetHostNames = make([]string, transferTargetNumber)

	for n := 0; n < transferChannelNumber; n++ {
		target := make([]*JzRsyncTarget, transferTargetNumber)
		for i := 0; i < transferTargetNumber; i++ {
			target[i] = &JzRsyncTarget{Target: &jzRsyncConfig.TargetServer[i]}
			target[i].Start()
			target[i].Name = fmt.Sprintf("%d-%d", n, i)
			obj.allTargetServer = append(obj.allTargetServer, target[i])

			if n == 0 {
				obj.AllTargetHostNames = append(obj.AllTargetHostNames, jzRsyncConfig.TargetServer[i].Group...)
			}
		}
		obj.transferChannel <- target
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

	<-obj.stopped
	<-obj.stopped

	for _, ts := range obj.allTargetServer {
		ts.Stop()
	}

	close(obj.transferChannel)

	JzLogger.Print("rsync stopped")
}

func (obj *JzRsync) pullTasks() {
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
				case <-obj.intervalToStopped:
					JzLogger.Print("catch intervalStopped signal")
					break F
				case <-interval.C:
					JzLogger.Print("catch pulltasks time event signal")
					obj.pullTasks()
				case <-newTask:
					JzLogger.Print("catch pulltasks redis event signal")
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
		case <-obj.taskToStopped:
			JzLogger.Print("catch taskToStopped signal")
			break E
		case task := <-obj.queue:
			targetServer := <-obj.transferChannel
			go Transfer(obj, targetServer, task)
		}
	}

	obj.stopped <- true
	JzLogger.Print("rsync exit")
}

func Transfer(obj *JzRsync, targetServer []*JzRsyncTarget, task *JzTask) {
	startTime := time.Now()
	JzLogger.Print("get task from queue", task)
	n := 0
	for _, hn := range task.HostNames {
		for _, ts := range targetServer {
			JzLogger.Printf("task id %d-%s will rsync for %s[%s][%s]", task.Id, hn, ts.Name, ts.Target.Name, ts.Target.Address)
			if hn != "*" && InStringArray(hn, ts.Target.Group) == false {
				n += 1
				JzLogger.Printf("task id %d-%s rsync ignore for %s[%s][%s]", task.Id, hn, ts.Name, ts.Target.Name, ts.Target.Address)
				continue
			}

			ok, err := ts.Rsync(task, task.RsyncMaxNum)
			if !ok {
				JzLogger.Print(err)
				continue
			}

			JzLogger.Printf("task id %d-%s rsync success for %s[%s][%s]", task.Id, hn, ts.Name, ts.Target.Name, ts.Target.Address)

			n += 1
		}
	}
	task.Done(n)
	JzLogger.Printf("transfer queue task %v done cost time %s", task, time.Since(startTime).String())
	GlobalData.TaskMap.Delete(task.Id)
	obj.transferChannel <- targetServer
}
