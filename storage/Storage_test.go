package storage

import (
	"fmt"
	"testing"
)

func TestDefaultInMemoryStoreConcurrentSafe(t *testing.T) {
	store := NewDefaultInMemoryStorageEngine()
	go func() {
		for i := 0; i < 5; i++ {
			if _, err := store.SaveUnit([]byte(fmt.Sprintf("data %d", i))); err != nil {
				t.Error(err)
			}
		}
	}()
	go func() {
		for i := 0; i < 5; i++ {
			data, err := store.FetchUnit()
			if err != nil {
				t.Error(err)
			}
			fmt.Println(string(data))
			fmt.Println("max offset ", store.MaxOffset())
		}
	}()
}

func TestDefaultInMemoryStore(t *testing.T) {
	store := NewDefaultInMemoryStorageEngine()
	for i := 0; i < 10; i++ {
		if _, err := store.SaveUnit([]byte(fmt.Sprintf("data %d", i))); err != nil {
			t.Error(err)
		}
	}
	for i := 0; i < 10; i++ {
		data, err := store.FetchUnit()
		if err != nil {
			t.Error(err)
		}
		fmt.Println(string(data))
		fmt.Println("max offset ", store.MaxOffset())
	}

}
