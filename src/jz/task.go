package jz

import "path"

type JzTask struct {
	Id int
	Name string
	Size int64
	Path string
	M5Sum string
	AbsolutePath string
	RelativePath string
	HostNames []string
	ExpectFinishedNum int
	RsyncMaxNum int
}

func (this *JzTask) Done(num int)  {
	if this.Id <= 0 {
		return
	}

	status := 500
	if this.ExpectFinishedNum <= num {
		status = 200
	}

	n, err := JzDaoInstance().UpdateTask(this.Id, status)
	if err == nil {
		JzLogger.Printf("update task %d success status=%d,affectedRows=%d", this.Id, status, n)
	} else {
		JzLogger.Printf("update task %d failed", this.Id)
	}
}

func (this *JzTask) Cancel(status int)  {
	if this.Id <= 0 {
		return
	}

	n, err := JzDaoInstance().UpdateTask(this.Id, status)
	if err == nil {
		JzLogger.Printf("update task %d success status=%d,affectedRows=%d", this.Id, status, n)
	} else {
		JzLogger.Printf("update task %d failed", this.Id)
	}
}

func AssembleTask(id int, file string) (*JzTask, error) {
	taskPath := path.Join(jzRsyncConfig.Repertory, file)
	n,err := GetFileSize(taskPath)
	if err != nil {
		JzLogger.Print(err)
		return nil, err
	}

	md5sum, err := GetFileMD5sum(taskPath)
	if err != nil {
		JzLogger.Print(err)
		return nil, err
	}

	taskDir := path.Dir(taskPath)
	taskName := path.Base(taskPath)

	taskRelativePath := path.Clean(taskPath[len(taskPath) - len(file):len(taskPath) - len(taskName)])
	if taskRelativePath == "." {
		taskRelativePath = ""
	}

	return &JzTask{
		Id:id,
		Name:taskName,
		Size:n,
		Path: taskPath,
		M5Sum:md5sum,
		AbsolutePath: taskDir,
		RelativePath: taskRelativePath,
		HostNames:[]string{},
		ExpectFinishedNum:len(jzRsyncConfig.TargetServer),
		RsyncMaxNum:3,
	}, nil
}