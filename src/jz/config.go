package jz

import (
	"errors"
	"os"
	"encoding/xml"
	"fmt"
)

type JzTargetServer struct {
	Name string `xml:"name"`
	Address string `xml:"address"`
}

type JzMysqlConfig struct {
	Ip string `xml:"ip"`
	Username string `xml:"username"`
	Password string `xml:"password"`
	Port uint `xml:"port"`
	Database string `xml:"database"`
}

type JzRsyncConfig struct {
	Address string `xml:"address"`
	Repertory string `xml:"repertory"`
	Interval int `xml:"interval"`
	TargetServer []JzTargetServer `xml:"target>server"`
	MysqlConfig JzMysqlConfig `xml:"mysql"`
}

var jzRsyncConfig *JzRsyncConfig

func ParseXmlConfig(path string) (*JzRsyncConfig, error) {
	if len(path) == 0 {
		return nil, errors.New("not found configure xml file")
	}

	n, err := GetFileSize(path)
	if  err !=nil || n == 0 {
		return nil, errors.New("not found configure xml file")
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	jzRsyncConfig = &JzRsyncConfig{}

	data := make([]byte, n)

	m, err := f.Read(data)
	if err != nil {
		return nil, err
	}

	if int64(m) != n {
		return nil, errors.New(fmt.Sprintf("expect read configure xml file size %d but result is %d", n, m))
	}

	err = xml.Unmarshal(data, &jzRsyncConfig)
	if err != nil {
		return nil, err
	}

	r, err := CheckFileIsDirectory(jzRsyncConfig.Repertory)
	if !r {
		return nil, err
	}

	return jzRsyncConfig, nil
}