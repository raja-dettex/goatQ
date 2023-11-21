package transports

import (
	"fmt"
	"io"
	"net"
	"strings"
)

type Peer interface {
}

type TCPPeer struct {
	Addr string
}

func (peer *TCPPeer) Broadcast(msg []byte) {
	conn, err := net.Dial("tcp", peer.Addr)
	defer conn.Close()
	if err != nil {
		return
	}
	_, err = conn.Write([]byte(fmt.Sprintf("WRITE %s", string(msg))))
	if err != nil {
		fmt.Println(err)
	}
	buff := make([]byte, 1024)
	for {
		n, err := conn.Read(buff)
		if err == io.EOF {
			fmt.Println("end of file reached , closing the connection")
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

func (peer *TCPPeer) ReadSync() {
	conn, err := net.Dial("tcp", peer.Addr)
	defer conn.Close()
	if err != nil {
		return
	}
	_, err = conn.Write([]byte("READ "))
	if err != nil {
		fmt.Println("checked spam", err)
		return
	}
	buff := make([]byte, 1024)
	for {
		n, err := conn.Read(buff)
		if err == io.EOF {
			fmt.Println("read sync end of line reached")
			return
		}
		if _, ok := err.(*net.OpError); ok {
			return
		}
		if err != nil {
			fmt.Println("error reading while syncing ", err)
		}
		res := strings.Split(string(buff[:n]), " ")
		if res[0] != "error" {
			fmt.Printf("synced peer (%s) with messae %s\n", peer.Addr, res[0])
			return
		}
	}

}
