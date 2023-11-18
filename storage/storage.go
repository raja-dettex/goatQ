package storage

import (
	"fmt"
	"sync"
)

type StorageEngine interface {
	SaveUnit([]byte) (int, error)
	FetchUnit() ([]byte, error)
}

type DefaultInMemoryStorageEngine struct {
	mutex    sync.RWMutex
	dataUnit [][]byte
}

func NewDefaultInMemoryStorageEngine() *DefaultInMemoryStorageEngine {
	return &DefaultInMemoryStorageEngine{
		dataUnit: make([][]byte, 0),
	}
}

func (engine *DefaultInMemoryStorageEngine) SaveUnit(data []byte) (int, error) {
	if data == nil {
		return -1, fmt.Errorf("empty messages can not be persisted")
	}
	engine.mutex.Lock()
	defer engine.mutex.Unlock()
	engine.dataUnit = append(engine.dataUnit, data)
	return len(engine.dataUnit) - 1, nil
}

func (engine *DefaultInMemoryStorageEngine) FetchUnit() ([]byte, error) {
	if engine.MaxOffset() < 0 {
		return nil, fmt.Errorf("queue is empty")
	}
	engine.mutex.RLock()
	defer engine.mutex.RUnlock()
	data := engine.dataUnit[0]
	engine.dataUnit = engine.dataUnit[1:]
	return data, nil
}

func (engine *DefaultInMemoryStorageEngine) MaxOffset() int {
	return len(engine.dataUnit) - 1
}
