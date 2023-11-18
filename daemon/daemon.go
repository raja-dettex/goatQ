package daemon

import (
	"fmt"
	"io"
	"net"

	"github.com/raja-dettex/goatQ/events"
	"github.com/raja-dettex/goatQ/storage"
)

type Opts struct {
	ListenAddr string
	TopicName  string
}

type storageType storage.StorageEngine

type Daemon struct {
	opts     Opts
	store    map[string]storageType
	Listener net.Listener
}

func NewDaemon(opts Opts) *Daemon {
	storage := storage.NewDefaultInMemoryStorageEngine()
	return &Daemon{
		opts:  opts,
		store: map[string]storageType{opts.TopicName: storage},
	}
}

func (daemon *Daemon) Start() error {
	ln, err := net.Listen("tcp", daemon.opts.ListenAddr)
	if err != nil {
		return err
	}
	fmt.Println("daemon listening in port ", daemon.opts.ListenAddr)
	daemon.Listener = ln
	go daemon.ConnectionLoop()
	return nil
}

func (daemon *Daemon) ConnectionLoop() {
	for {
		conn, err := daemon.Listener.Accept()
		if err != nil {
			fmt.Println(err)
		}
		go daemon.handleConn(conn)
	}
}

func (daemon *Daemon) handleConn(conn net.Conn) {
	buff := make([]byte, 1024)
	for {
		n, err := conn.Read(buff)
		if err == io.EOF {
			fmt.Println("Closing connection , end of line reached")
			break
		}
		if err != nil {
			fmt.Println("conn read error ", err)
		}
		msg := buff[:n]
		daemon.handleMessage(msg, conn)
	}
}

func (daemon *Daemon) handleMessage(msg []byte, conn net.Conn) {
	event, err := events.ParseEvent(msg)
	if err != nil {
		fmt.Println(err)
	}
	switch event.Command {
	case events.EVENTRead:
		data, err := daemon.Read()
		if err != nil {
			conn.Write([]byte(fmt.Sprintf("error reading from queue %v", err)))
		}
		conn.Write(data)
	case events.EVENTWrite:
		offset, err := daemon.Write(event.Value)
		if err != nil {
			conn.Write([]byte(fmt.Sprintf("error writing to the queue %v", err)))
		}
		conn.Write([]byte(fmt.Sprintf("writing to conn : offset %d", offset)))
	}
}

func (daemon *Daemon) Read() ([]byte, error) {
	storage, ok := daemon.store[daemon.opts.TopicName]
	if !ok {
		return nil, fmt.Errorf("storage with the given topic name does not exist %s", daemon.opts.TopicName)
	}
	return storage.FetchUnit()

}

func (daemon *Daemon) Write(data []byte) (int, error) {
	storage, ok := daemon.store[daemon.opts.TopicName]
	if !ok {
		return -1, fmt.Errorf("storage with the given topic name does not exist %s", daemon.opts.TopicName)
	}
	return storage.SaveUnit(data)
}
