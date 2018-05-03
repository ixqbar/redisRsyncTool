package jz

import "sync"

type TGlobalData struct {
	TaskMap *sync.Map
}

var GlobalData = &TGlobalData{
	TaskMap:new(sync.Map),
}
