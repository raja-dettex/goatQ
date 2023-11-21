package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"time"

	"github.com/raja-dettex/goatQ/daemon"
)

func main() {
	listenAddr := os.Getenv("LISTEN_ADDR")
	masterAddr := os.Getenv("MASTER_ADDR")
	opts := daemon.Opts{ListenAddr: listenAddr, TopicName: "test", MasterAddr: masterAddr}
	d := daemon.NewDaemon(opts)
	if err := d.Start(); err != nil {
		log.Fatal(err)
	}
	time.Sleep(time.Second * 3)
	// go func() {
	// 	for i := 0; i < 5; i++ {
	// 		go sendWriteMessagetoSocket(fmt.Sprintf("WRITE hello%d", i))
	// 	}
	// }()

	//time.Sleep(time.Second * 3)
	// go func() {
	// 	for {
	// 		time.Sleep(time.Second * 3)
	// 		go sendReadMessagetoSocket()
	// 	}
	// }()

	select {}
}

func sendReadMessagetoSocket() {
	conn, err := net.Dial("tcp", ":3000")
	if err != nil {
		fmt.Println(err)
	}
	_, err = conn.Write([]byte("READ "))
	if err != nil {
		fmt.Println(err)
	}
	buff := make([]byte, 1024)
	for {
		n, err := conn.Read(buff)
		if err != nil {
			fmt.Println("Read from the connection error ", err)
		}
		fmt.Println(string(buff[:n]))
	}
}

func sendWriteMessagetoSocket(message string) {
	conn, err := net.Dial("tcp", ":3000")
	if err != nil {
		fmt.Println(err)
	}
	_, err = conn.Write([]byte(message))
	if err != nil {
		fmt.Println(err)
	}
	buff := make([]byte, 1024)
	for {
		n, err := conn.Read(buff)
		if err != nil {
			fmt.Println("write to the connection error ", err)
		}
		fmt.Println(string(buff[:n]))
	}
}
