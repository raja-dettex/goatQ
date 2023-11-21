package daemon

import (
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/raja-dettex/goatQ/events"
	"github.com/raja-dettex/goatQ/storage"
	"github.com/raja-dettex/goatQ/transports"
)

type Opts struct {
	ListenAddr string
	TopicName  string
	MasterAddr string
}

type storageType storage.StorageEngine

type Daemon struct {
	opts     Opts
	store    map[string]storageType
	Listener net.Listener
	isMaster bool

	mutex    sync.RWMutex
	peers    map[string]*transports.TCPPeer
	peerPool []*transports.TCPPeer
}

func NewDaemon(opts Opts) *Daemon {
	storage := storage.NewDefaultInMemoryStorageEngine()
	daemon := &Daemon{
		opts:     opts,
		store:    map[string]storageType{opts.TopicName: storage},
		peers:    make(map[string]*transports.TCPPeer),
		isMaster: opts.MasterAddr == "",
	}
	if opts.MasterAddr == "" {
		daemon.peerPool = make([]*transports.TCPPeer, 0)
	}
	return daemon
}

func (daemon *Daemon) Start() error {
	ln, err := net.Listen("tcp", daemon.opts.ListenAddr)
	if err != nil {
		return err
	}
	fmt.Println("daemon listening in port ", daemon.opts.ListenAddr)
	daemon.Listener = ln
	if !daemon.isMaster {
		go daemon.joinMaster()
		//go daemon.broadCastMessage()
	}
	go daemon.ConnectionLoop()
	return nil
}

func (daemon *Daemon) filterPeerPool() {
	for addr, peer := range daemon.peers {
		conn, err := net.DialTimeout("tcp", peer.Addr, time.Second*2)
		if err != nil {
			delete(daemon.peers, addr)
			continue
		}
		conn.Close()
	}
}

func (daemon *Daemon) broadCastMessage(msg []byte) {
	daemon.filterPeerPool()
	for _, peer := range daemon.peers {
		fmt.Println(peer)
		peer.Broadcast(msg)
	}

}

func (daemon *Daemon) joinMaster() {
	conn, err := net.Dial("tcp", daemon.opts.MasterAddr)
	defer conn.Close()
	if err != nil {
		fmt.Println(err)
	}
	daemon.mutex.RLock()
	_, ok := daemon.peers[daemon.opts.MasterAddr]
	daemon.mutex.RUnlock()
	if ok {
		fmt.Println("peer exists")
		return
	}

	daemon.mutex.Lock()
	daemon.peers[daemon.opts.MasterAddr] = &transports.TCPPeer{Addr: daemon.opts.MasterAddr}
	daemon.mutex.Unlock()
	fmt.Println(daemon.peers)
	_, err = conn.Write([]byte(fmt.Sprintf("JOIN %s", daemon.opts.ListenAddr)))
	if err != nil {
		fmt.Println(err)
	}
	buff := make([]byte, 1024)
	for {
		n, err := conn.Read(buff)
		if err == io.EOF {
			fmt.Println("End of file reached, closing the connection")
			return
		}
		if _, ok := err.(*net.OpError); ok {
			return
		}
		if err != nil {
			fmt.Println(err)
		}
		fmt.Println(string(buff[:n]))
	}
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
	defer conn.Close()
	for {
		n, err := conn.Read(buff)
		if err == io.EOF {
			fmt.Println("Closing connection , end of line reached")
			return
		}
		if _, ok := err.(*net.OpError); ok {
			return
		}
		if err != nil {
			fmt.Println("conn read error ", err)
		}
		msg := buff[:n]
		go daemon.handleMessage(msg, conn)
	}
}

func (daemon *Daemon) handleMessage(msg []byte, conn net.Conn) {
	event, err := events.ParseEvent(msg)
	if err != nil {
		fmt.Println(err)
	}
	switch event.Command {
	case events.EVENTJoin:
		if daemon.isMaster {
			if _, err := conn.Write([]byte("succeesfully joined the master node")); err != nil {
				fmt.Println(err)
			}
			daemon.mutex.RLock()
			_, ok := daemon.peers[event.RemoteAddr]
			daemon.mutex.RUnlock()
			if ok {
				fmt.Println("peer already in the list")
				conn.Close()
				return
			}
			daemon.mutex.Lock()
			daemon.peers[event.RemoteAddr] = &transports.TCPPeer{Addr: event.RemoteAddr}
			daemon.peerPool = append(daemon.peerPool, &transports.TCPPeer{Addr: event.RemoteAddr})
			daemon.mutex.Unlock()
			fmt.Println(daemon.peerPool)
			conn.Close()
			return
		}
		conn.Close()
		return
	case events.EVENTRead:
		data, err := daemon.Read()
		if err != nil {
			conn.Write([]byte(fmt.Sprintf("error reading from queue %v", err)))
		}
		if _, err := conn.Write(data); err != nil {
			fmt.Println(err)
		}
		if daemon.isMaster {
			go daemon.syncPeers()
		}
		conn.Close()
		return
	case events.EVENTWrite:
		offset, err := daemon.Write(event.Value)
		if err != nil {
			conn.Write([]byte(fmt.Sprintf("error writing to the queue %v", err)))
		}
		if _, err := conn.Write([]byte(fmt.Sprintf("writing to conn : offset %d", offset))); err != nil {
			fmt.Println(err)
		}
		if daemon.isMaster {
			daemon.broadCastMessage(event.Value)
		}
		conn.Close()
		return
	}
}

func (daemon *Daemon) syncPeers() {
	daemon.filterPeerPool()
	for _, peer := range daemon.peers {
		peer.ReadSync()
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
